package org.everthrift.elastic;

import com.google.common.base.Throwables;
import org.elasticsearch.index.VersionType;
import org.everthrift.appserver.model.RoModelFactoryIF;
import org.everthrift.appserver.utils.jmx.BeanNameHolder;
import org.everthrift.appserver.utils.jmx.RuntimeJmxNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@ManagedResource(objectName = "bean:name=replaced")
public interface EsProviderIF<PK extends Serializable, ENTITY extends EsIndexableIF, E extends Exception> extends RoModelFactoryIF<PK, ENTITY, E>, RuntimeJmxNames, BeanNameAware, BeanNameHolder, ApplicationContextAware {

    final static Logger _log = LoggerFactory.getLogger(EsProviderIF.class);

    void fetchAll(final int batchSize, Consumer<List<ENTITY>> consumer);

    void invalidateLocal(PK id);

    String getIndexName();

    String getMappingName();

    ApplicationContext getApplicationContext();

    default Set<String> getIndexTriggers() {
        return null;
    }

    default VersionType getVersionType() {
        return VersionType.EXTERNAL_GTE;
    }

    default void indexInES(ENTITY e) {
        getApplicationContext().getBean(Indexer.class).scheduleIndex(getBeanName(), e);
    }

    default void indexInES(PK pk, long version) {
        getApplicationContext().getBean(Indexer.class).scheduleIndex(getBeanName(), new EsIndexableImpl(pk, version));
    }

    // Only for dev
    default void fetchIndexAllInES(ExecutorService executor, int batchSize) {
        executor.submit(() -> {

            final AtomicInteger size = new AtomicInteger(0);
            try {
                fetchAll(batchSize, entities -> {
                    final Indexer indexer = getApplicationContext().getBean(Indexer.class);
                    indexer.runIndexTasks(indexer.buildIndexTasks(getBeanName(), entities));
                    final int indexed = size.addAndGet(entities.size());
                    if (indexed % 1000 == 0) {
                        _log.info("Indexing {}/{}: {}", indexer.getIndexPrefix() + getIndexName(), getMappingName(), indexed);
                    }
                });
            } catch (Exception e) {
                _log.error("Exception in fetchIndexAllInES", e);
                throw Throwables.propagate(e);
            }

            final Indexer indexer = getApplicationContext().getBean(Indexer.class);
            _log.info("Finished indexing {}/{}:{}", indexer.getIndexPrefix() + getIndexName(), getMappingName(), size.get());
        });
    }

    @ManagedOperation
    default void createIndex() throws IOException {
        final Indexer indexer = getApplicationContext().getBean(Indexer.class);
        indexer.deleteIndex(getIndexName());
        indexer.createIndex(getIndexName());
    }

    @ManagedOperation
    default void putMapping() throws IOException {
        getApplicationContext().getBean(Indexer.class).putMapping(getIndexName(), getMappingName());
    }

    @ManagedOperation
    default void indexAll(int limit) {

        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be >=0");
        }

        final Map<String, ExecutorService> executors = getApplicationContext().getBeansOfType(ExecutorService.class);

        fetchIndexAllInES(Optional.ofNullable(executors.get("executor"))
                                  .orElseGet(() -> executors.values().stream().findFirst().get()),
                          limit);
    }

    @Override
    default String getJmxName() {
        return getBeanName();
    }

    @Override
    default String[] getJmxPath() {
        return new String[]{"elastic"};
    }

    default PK parsePk(String esId) {
        return (PK) esId;
    }

    default String getEsId(PK pk) {
        return (String) pk;
    }
}
