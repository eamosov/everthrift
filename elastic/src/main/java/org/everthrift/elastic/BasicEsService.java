package org.everthrift.elastic;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.NotImplementedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.everthrift.appserver.model.AsyncRoModelFactoryIF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public abstract class BasicEsService {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected Client esClient;

    @Value("${es.index.prefix}")
    private String indexPrefix;

    abstract protected <T> T deserialize(String s, Class<T> cls);

    public static class ESearchResult<PK extends Serializable, ENTITY extends EsIndexableIF> {
        public final int total;
        public final List<ENTITY> sources;

        public final List<SearchHit> hits;
        public final List<SearchHit> innerHits;
        public final SearchResponse response;
        private final EsProviderIF<PK, ENTITY> factory;
        public final List<ENTITY> loaded;
        private List<PK> ids;

        ESearchResult(EsProviderIF<PK, ENTITY> factory, SearchResponse response, int total, List<SearchHit> hits, List<SearchHit> innerHits, List<ENTITY> sources) {
            super();
            this.total = total;
            this.hits = hits;
            this.innerHits = innerHits;
            this.sources = sources;
            this.response = response;
            this.factory = factory;
            this.ids = null;
            this.loaded = null;
        }

        ESearchResult(ESearchResult<PK, ENTITY> other, List<ENTITY> loaded) {
            this.total = other.total;
            this.hits = other.hits;
            this.innerHits = other.innerHits;
            this.sources = other.sources;
            this.response = other.response;
            this.factory = other.factory;
            this.ids = other.ids;
            this.loaded = loaded;
        }

        public static <PK extends Serializable, ENTITY extends EsIndexableIF> ESearchResult<PK, ENTITY> empty(EsProviderIF<PK, ENTITY> factory) {
            return new ESearchResult<>(factory, null, 0, Collections.emptyList(), Collections.emptyList(), Collections
                .emptyList());
        }

        public List<PK> getIds() {
            return Optional.ofNullable(ids).orElseGet(() -> (ids = hits.stream()
                                                                       .map(sh -> factory.parsePk(sh.id()))
                                                                       .collect(Collectors.toList())));
        }

        public List<ENTITY> loadEntities() {
            return factory.findEntityByIdsInOrder(getIds());
        }

        @SuppressWarnings("unchecked")
        public CompletableFuture<ESearchResult<PK, ENTITY>> loadEntitiesAsync() {
            if (!(factory instanceof AsyncRoModelFactoryIF)) {
                throw new NotImplementedException("Factory " + factory.getClass()
                                                                      .getCanonicalName() + " must implement AsyncRoModelFactoryIF");
            }

            return ((AsyncRoModelFactoryIF<PK, ENTITY>) factory).findEntityByIdsInOrderAsync(getIds())
                                                                .thenApply(loaded -> new ESearchResult<>(ESearchResult.this, loaded));
        }

    }

    public <PK extends Serializable, ENTITY extends EsIndexableIF> ESearchResult<PK, ENTITY> searchQuery(EsProviderIF<PK, ENTITY> factory, SearchType searchType, boolean scroll, SearchSourceBuilder sourceBuilder) throws ElasticsearchException {
        return searchQuery(factory, searchType, scroll, null, sourceBuilder, false);
    }

    public <PK extends Serializable, ENTITY extends EsIndexableIF> ESearchResult<PK, ENTITY> searchQuery(EsProviderIF<PK, ENTITY> factory, SearchType searchType, boolean scroll, SearchSourceBuilder sourceBuilder, boolean parseSource)
        throws ElasticsearchException {
        return searchQuery(factory, searchType, scroll, null, sourceBuilder, parseSource);
    }

    public <PK extends Serializable, ENTITY extends EsIndexableIF> ESearchResult<PK, ENTITY> searchQuery(EsProviderIF<PK, ENTITY> factory, SearchType searchType, boolean scroll, String innerName, SearchSourceBuilder sourceBuilder,
                                                                                                         boolean parseSource) throws ElasticsearchException {

        try {
            return searchQueryAsync(factory, searchType, scroll, innerName, sourceBuilder, parseSource).get();
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause(), ElasticsearchException.class);
            throw Throwables.propagate(e);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }

    }

    private <PK extends Serializable, ENTITY extends EsIndexableIF> void proccessHits(final ESearchResult<PK, ENTITY> ret, SearchResponse response, String innerName, Class<ENTITY> entityClass) {
        for (SearchHit h : response.getHits().getHits()) {
            ret.hits.add(h);

            if (innerName != null) {
                for (SearchHit i : h.getInnerHits().get(innerName)) {
                    ret.innerHits.add(i);
                }
            }

            if (entityClass != null) {
                ret.sources.add(deserialize(h.getSourceAsString(), entityClass));
            }
        }
    }

    private <PK extends Serializable, ENTITY extends EsIndexableIF> CompletableFuture<ESearchResult<PK, ENTITY>> scroll(final ESearchResult<PK, ENTITY> ret, final String scrollId, String innerName, Class<ENTITY> entityClass) {

        final CompletableFuture<ESearchResult<PK, ENTITY>> f = new CompletableFuture<>();

        esClient.searchScroll(esClient.prepareSearchScroll(scrollId)
                                      .setScroll(TimeValue.timeValueMinutes(1))
                                      .request(), new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {

                log.trace("Scroll response: {}", response);

                if (response.getHits().getHits().length == 0) {
                    f.complete(ret);
                    esClient.clearScroll(esClient.prepareClearScroll().addScrollId(scrollId).request());
                    return;
                }

                proccessHits(ret, response, innerName, entityClass);
                scroll(ret, response.getScrollId(), innerName, entityClass).thenAccept(f::complete);
            }

            @Override
            public void onFailure(Exception e) {
                esClient.clearScroll(esClient.prepareClearScroll().addScrollId(scrollId).request());
                f.completeExceptionally(e);
            }
        });

        return f;
    }

    public <PK extends Serializable, ENTITY extends EsIndexableIF> CompletableFuture<ESearchResult<PK, ENTITY>> searchQueryAsync(EsProviderIF<PK, ENTITY> factory, SearchType searchType, boolean scroll, String innerName,
                                                                                                                                 SearchSourceBuilder sourceBuilder, boolean parseSource) throws ElasticsearchException {

        log.trace("query: {}", sourceBuilder.toString());

        final SearchRequest sr = new SearchRequest();

        sr.indices(indexPrefix + factory.getIndexName());
        sr.types(factory.getMappingName());
        sr.source(sourceBuilder);
        sr.searchType(searchType);

        if (scroll) {
            sr.scroll(TimeValue.timeValueMinutes(1));
        }

        final CompletableFuture<ESearchResult<PK, ENTITY>> f = new CompletableFuture<>();

        esClient.search(sr, new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {

                log.trace("Response: {}", response);

                final int total = (int) response.getHits().getTotalHits();

                final ESearchResult<PK, ENTITY> ret = new ESearchResult<>(factory, response, total, Lists.newArrayList(), innerName == null ? null : Lists
                    .newArrayList(),
                                                                          parseSource ? Lists.newArrayList() : null);

                if (total == 0) {
                    f.complete(ret);
                    return;
                }

                proccessHits(ret, response, innerName, parseSource ? factory.getEntityClass() : null);

                if (scroll && response.getScrollId() != null) {
                    scroll(ret, response.getScrollId(), innerName, parseSource ? factory.getEntityClass() : null).thenAccept(f::complete);
                } else {
                    f.complete(ret);
                }
            }

            @Override
            public void onFailure(Exception e) {
                f.completeExceptionally(e);
            }
        });

        return f;
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    public void setIndexPrefix(String indexPrefix) {
        this.indexPrefix = indexPrefix;
    }
}
