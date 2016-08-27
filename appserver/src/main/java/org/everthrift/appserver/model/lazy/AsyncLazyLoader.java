package org.everthrift.appserver.model.lazy;

import com.google.common.base.Throwables;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface AsyncLazyLoader<K> extends LazyLoader<K> {

    @Override
    default int process(List<K> entities) {
        try {
            return processAsync(entities).get();
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    CompletableFuture<Integer> processAsync(List<K> entities);
}
