package org.everthrift.elastic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.collections.map.LazyMap;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.VersionType;
import org.everthrift.appserver.model.LocalEventBus;
import org.everthrift.appserver.model.events.InsertEntityEvent;
import org.everthrift.appserver.model.events.UpdateEntityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.util.CollectionUtils;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

//TODO сделать удаление из индекса
//TODO сделать обновление индексируемых сущностей по сравнению индексируемых полей
public abstract class Indexer implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(Indexer.class);

    @Autowired
    private LocalEventBus localEventBus;

    @Autowired
    protected ApplicationContext context;

    @Autowired
    protected Client esClient;

    private Thread thread;

    @Autowired
    private boolean testMode;

    @Value("${es.index.prefix}")
    private String indexPrefix;

    protected abstract void addToQueue(List<? extends IndexTaskIF> tasks) throws TException;

    protected abstract IndexTaskIF createIndexTask();

    protected abstract IndexTaskIF createIndexTask(int operation, String indexName, String mappingName, int versionType, long version, String id, String source, String parentId);

    protected abstract void beforeSerialize(List<EsIndexableIF> objs);

    protected abstract String serialize(EsIndexableIF o);

    // factoryName -> (id -> version)
    @SuppressWarnings("unchecked")
    private Map<String, Map<Serializable, EsIndexableIF>> batch = LazyMap.decorate(Maps.newHashMap(), () -> Maps.newHashMap());

    private synchronized void batchPut(String factoryName, List<EsIndexableIF> ids) {
        final Map<Serializable, EsIndexableIF> f = batch.get(factoryName);
        for (EsIndexableIF id : ids) {
            final EsIndexableIF old = f.get(id.getPk());
            if (old == null || old.getVersion() < id.getVersion()) {
                f.put(id.getPk(), id);
            }
        }
    }

    /**
     * @return factoryName -> List<EsIndexableIF>
     */
    private synchronized Map<String, List<EsIndexableIF>> batchGet() {

        if (batch.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, List<EsIndexableIF>> r = batch.entrySet()
                                                        .stream()
                                                        .collect(toMap(e -> e.getKey(), e -> e.getValue()
                                                                                              .values()
                                                                                              .stream()
                                                                                              .collect(toList())));
        batch.clear();
        return r;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        localEventBus.register(this);

        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                do {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        processBatch();
                        return;
                    }
                    processBatch();
                } while (true);
            }
        });

        thread.start();
    }

    @Subscribe
    public void onIndexEvent(InsertEntityEvent<?, ?> event) {

        if (event.factory instanceof EsProviderIF) {

            if (!(event.entity instanceof EsIndexableIF)) {
                log.error("Entity {} must be instanceof EsIndexableIF", event.entity.getClass().getCanonicalName());
            }

            scheduleIndex(((EsProviderIF<?, ?>) event.factory).getBeanName(), (EsIndexableIF) event.entity);
        }
    }

    @Subscribe
    public void onProperyUpdateEvent(UpdateEntityEvent<?, ?> event) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        if (event.factory instanceof EsProviderIF) {
            final Set<String> triggers = ((EsProviderIF<?, ?>) event.factory).getIndexTriggers();

            if (event.updatedByPk != null) {
                scheduleIndex(((EsProviderIF<?, ?>) event.factory).getBeanName(), new EsIndexableImpl(event.updatedByPk, 0));
            } else if (triggers == null) {
                if (!event.afterUpdate.equals(event.beforeUpdate)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Detected changed(not equals) for entity of class {} pk:{}",
                                  event.factory.getEntityClass().getSimpleName(),
                                  event.afterUpdate.getPk());
                    }

                    scheduleIndex(((EsProviderIF<?, ?>) event.factory).getBeanName(), (EsIndexableIF) event.afterUpdate);
                }
            } else {
                for (String propertyName : triggers) {
                    final PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(event.factory.getEntityClass(), propertyName);
                    final Object before = event.beforeUpdate == null ? null : pd.getReadMethod()
                                                                                .invoke(event.beforeUpdate);
                    final Object after = event.afterUpdate == null ? null : pd.getReadMethod()
                                                                              .invoke(event.afterUpdate);

                    if (!Objects.equals(before, after)) {

                        if (log.isDebugEnabled()) {
                            log.debug("Detected changed property '{}' for entity of class {} pk:{}", propertyName, event.factory
                                .getEntityClass()
                                .getSimpleName(), event.afterUpdate.getPk());
                        }

                        scheduleIndex(((EsProviderIF<?, ?>) event.factory).getBeanName(), (EsIndexableIF) event.afterUpdate);
                    }
                }
            }
        }
    }

    public void scheduleIndex(String factoryName, Serializable pk, long version) {
        scheduleIndex(factoryName, new EsIndexableImpl(pk, version));
    }

    public void scheduleIndex(String factoryName, EsIndexableIF e) {
        scheduleIndex(factoryName, Collections.singletonList(e));
    }

    public void scheduleIndex(String factoryName, List<EsIndexableIF> ids) {

        log.debug("scheduleIndex: factory={}, ids={}", factoryName, ids);

        if (!ids.isEmpty()) {
            batchPut(factoryName, ids);
        }

        if (testMode) {
            processBatch();
        }
    }

    private void processBatch() {

        final Map<String, List<EsIndexableIF>> batch = batchGet();

        if (!batch.isEmpty()) {
            log.debug("processBatch: {}", batch);

            batch.forEach((k, v) -> {
                try {
                    final List<IndexTaskIF> tasks = buildIndexTasks(k, v);

                    if (!tasks.isEmpty()) {
                        addToQueue(tasks);
                    }

                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public List<IndexTaskIF> buildIndexTasks(String factoryName, List<? extends EsIndexableIF> ids) {
        log.debug("runIndex: factory={}, ids={}", factoryName, ids);

        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }

        final EsProviderIF<Serializable, EsIndexableIF> factory = (EsProviderIF<Serializable, EsIndexableIF>) context.getBean(factoryName, EsProviderIF.class);

        if (factory == null) {
            log.error("Couldn't find factory: {}", factoryName);
            return Collections.emptyList();
        }

        final Map<Serializable, EsIndexableIF> loaded = factory.findEntityByIdAsMap(ids.stream()
                                                                                       .map(EsIndexableIF::getPk)
                                                                                       .collect(Collectors.toSet()));

        final List<IndexTaskIF> tasks = Lists.newArrayListWithCapacity(loaded.size());
        final List<EsIndexableIF> toIndex = Lists.newArrayListWithCapacity(loaded.size());

        for (EsIndexableIF id : ids) {

            EsIndexableIF o = loaded.get(id.getPk());
            if (o != null && o.getVersion() >= id.getVersion()) {
                toIndex.add(o);
            } else if (o == null) {
                tasks.add(createIndexTask(EsOp.DELETE,
                                          indexPrefix + factory.getIndexName(),
                                          factory.getMappingName(),
                                          factory.getVersionType().getValue(),
                                          0,
                                          factory.getEsId(id.getPk()),
                                          null,
                                          null));
            } else {
                log.debug("o.version={}, id.version={}, id.pk={}, factory={}", o.getVersion(), id.getVersion(), id.getPk(), factory
                    .getClass()
                    .getSimpleName());

                factory.invalidateLocal(id.getPk());
                o = factory.findEntityById(id.getPk());

                if (o == null) {
                    log.error("coudn't load object with id={}", id.getPk());
                } else if (o.getVersion() < id.getVersion()) {
                    log.error("loaded o.version={} < id.version={}, pk={}, factory={}", o.getVersion(), id.getVersion(), id
                        .getPk(), factory.getClass().getSimpleName());
                } else {
                    toIndex.add(o);
                }
            }
        }

        beforeSerialize(toIndex);

        tasks.addAll(toIndex.stream()
                            .map(o -> createIndexTask(EsOp.INDEX,
                                                      indexPrefix + factory.getIndexName(),
                                                      factory.getMappingName(),
                                                      factory.getVersionType()
                                                             .getValue(),
                                                      o.getVersion(),
                                                      factory.getEsId(o.getPk()),
                                                      serialize(o),
                                                      o instanceof EsParentAwareIF ? ((EsParentAwareIF) o).getEsParent() : null))
                            .collect(Collectors.toSet()));

        if (tasks.isEmpty()) {
            return Collections.emptyList();
        }

        return tasks;
    }

    public void runIndexTasks(List<? extends IndexTaskIF> tasks) {

        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }

        final long startMillis = System.currentTimeMillis();

        final BulkRequestBuilder bulkRequest = esClient.prepareBulk();

        for (IndexTaskIF t : tasks) {

            switch (t.getOperation()) {
                case EsOp.INDEX:
                    final IndexRequestBuilder rb = esClient.prepareIndex(t.getIndexName(), t.getMappingName(), t.getId())
                                                           .setSource(t.getSource())
                                                           .setVersionType(VersionType.fromValue((byte) t.getVersionType()))
                                                           .setVersion(t.getVersion());

                    if (t.getParentId() != null) {
                        rb.setParent(t.getParentId());
                    }

                    bulkRequest.add(rb);
                    break;
                case EsOp.DELETE:
                    bulkRequest.add(esClient.prepareDelete(t.getIndexName(), t.getMappingName(), t.getId()));
                    break;
            }
        }

        bulkRequest.setRefresh(true);

        final BulkResponse response = bulkRequest.get();

        log.debug("index took {} millis for indexing {} entities", System.currentTimeMillis() - startMillis, tasks.size());

        if (response.hasFailures()) {

            boolean hasUnknownErrors = false;

            for (int i = 0; i < response.getItems().length; i++) {
                final BulkItemResponse resp = response.getItems()[i];
                if (resp.isFailed()) {
                    if (resp.getFailureMessage().contains("VersionConflictEngineException")) {
                        log.warn("Version conflict while index:{}", resp.getFailureMessage());
                    } else {
                        hasUnknownErrors = true;
                    }
                }
            }

            if (hasUnknownErrors) {
                log.error("failures: {}", response.buildFailureMessage());
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        thread.interrupt();
        thread.join();
    }

    public void deleteIndex(String indexName) {

        log.info("Delete index: {}", indexPrefix + indexName);
        try {
            esClient.admin().indices().prepareDelete(indexPrefix + indexName).execute().actionGet();
        } catch (Exception e) {
            log.error("deleteIndex", e);
        }
    }

    private String resourceContent(String name) throws IOException {
        return IOUtils.toString(context.getResource(name).getInputStream());
    }

    public void createIndex(String indexName) throws IOException {
        createIndex(indexPrefix + indexName, resourceContent(String.format("classpath:es/%s/_settings.json", indexName)));
        putMapping(indexName, "_default_");
    }

    private void createIndex(String indexName, String settings) {

        log.info("Create index: {}", indexName);

        try {
            esClient.admin().indices().prepareCreate(indexName).setSource(settings).execute().actionGet();
        } catch (Exception e) {
            log.error("createIndex", e);
        }
    }

    public void putMapping(String indexName, String typeName) throws IOException {
        putMapping(indexPrefix + indexName, typeName, resourceContent(String.format("classpath:es/%s/%s.json", indexName, typeName)));
    }

    private void putMapping(String indexName, String typeName, String mapping) {

        log.info("Put mapping: {}.{}", indexName, typeName);

        try {
            esClient.admin()
                    .indices()
                    .preparePutMapping(indexName)
                    .setType(typeName)
                    .setSource(mapping)
                    .execute()
                    .actionGet();
        } catch (Exception e) {
            log.error("putMapping", e);
        }
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    public void setIndexPrefix(String indexPrefix) {
        this.indexPrefix = indexPrefix;
    }
}
