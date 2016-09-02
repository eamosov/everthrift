package org.everthrift.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Created by fluder on 27.08.16.
 */
public class FuturesSupport {

    public static <V> CompletableFuture<List<V>> allOf(CompletableFuture<? extends V>... futures) {
        return CompletableFuture
            .allOf(futures)
            .thenApply(v -> Arrays.stream(futures)
                                  .map(CompletableFuture::join)
                                  .collect(Collectors.toList())
            );

    }

    public static <V> CompletableFuture<List<V>> allOf(Collection<? extends CompletableFuture<? extends V>> futures) {

        return CompletableFuture
            .allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenApply(v -> futures.stream()
                                   .map(CompletableFuture::join)
                                   .collect(Collectors.toList())
            );

    }

    private static <I, O> void _transformAsync(I result, Throwable t, final CompletableFuture<O> ret, CompletableAsyncFunction<? super I, ? extends O> function) {
        if (t != null) {
            ret.completeExceptionally(t);
        } else {
            try {
                function.apply(result).whenComplete((result2, t2) -> {
                    if (t2 != null) {
                        ret.completeExceptionally(t2);
                    } else {
                        ret.complete(result2);
                    }
                });
            } catch (Throwable e) {
                ret.completeExceptionally(e);
            }
        }
    }

    public static <I, O> CompletableFuture<O> transformAsync(CompletableFuture<I> input, CompletableAsyncFunction<? super I, ? extends O> function) {
        final CompletableFuture<O> ret = new CompletableFuture<O>();

        input.whenComplete((result, t) -> {
            _transformAsync(result, t, ret, function);
        });

        return ret;
    }

    public static <I, O> CompletableFuture<O> transformAsync(CompletableFuture<I> input, CompletableAsyncFunction<? super I, ? extends O> function, Executor executor) {
        final CompletableFuture<O> ret = new CompletableFuture<O>();

        input.whenCompleteAsync((result, t) -> {
            _transformAsync(result, t, ret, function);
        }, executor);

        return ret;
    }

    public static <V> CompletableFuture<V> immediateFailedFuture(Throwable throwable) {
        final CompletableFuture<V> ret = new CompletableFuture<V>();
        ret.completeExceptionally(throwable);
        return ret;
    }
}
