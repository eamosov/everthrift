package org.everthrift.utils;

import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncRegister<F extends CompletableFuture> {


    private final Map<Integer, Pair<F, ScheduledFuture>> callbacks = new HashMap<>();

    private final AtomicInteger seqId = new AtomicInteger();

    private final ScheduledExecutorService scheduller;

    public AsyncRegister(ScheduledExecutorService scheduller) {
        this.scheduller = scheduller;
    }

    public synchronized F pop(int seqId) {
        final Pair<F, ScheduledFuture> p = callbacks.remove(seqId);

        if (p == null) {
            return null;
        }

        if (p.second != null) {
            p.second.cancel(false);
        }

        return p.first.isDone() ? null : p.first;
    }

    public synchronized List<F> popAll() {
        final List<F> ret = new ArrayList<>();

        for (Pair<F, ScheduledFuture> p : callbacks.values()) {

            if (p.second != null) {
                p.second.cancel(false);
            }

            if (!p.first.isDone()) {
                ret.add(p.first);
            }
        }
        callbacks.clear();
        return ret;
    }

    public synchronized void put(final int seqId, F ii, final long tmMs) {
        callbacks.put(seqId, new Pair<>(ii, scheduller.schedule(() -> {

            final Pair<F, ScheduledFuture> p;

            synchronized (AsyncRegister.this) {
                p = callbacks.remove(seqId);
            }

            p.first.completeExceptionally(new TTransportException(TTransportException.TIMED_OUT, "TIMED_OUT"));

        }, tmMs, TimeUnit.MILLISECONDS)));
    }

    public synchronized void put(int seqId, F ii) {
        callbacks.put(seqId, new Pair<>(ii, null));
    }

    public int nextSeqId() {
        return seqId.incrementAndGet();
    }

}
