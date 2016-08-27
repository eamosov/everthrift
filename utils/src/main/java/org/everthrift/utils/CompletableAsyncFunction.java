package org.everthrift.utils;

import java.util.concurrent.CompletableFuture;

/**
 * Created by fluder on 27.08.16.
 */
public interface CompletableAsyncFunction<I, O> {
    CompletableFuture<O> apply(I input);
}
