package org.everthrift.thrift;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.thrift.transport.TTransportException;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncRegister {

    private static class Pair<T, V> implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 5839335947801602957L;

        public final T first;

        public final V second;

        public static <T, V> Pair<T, V> create(T first, V second) {
            return new Pair<T, V>(first, second);
        }

        public Pair(T first, V second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int hashCode() {
            return (first == null ? 11 : first.hashCode()) + (second == null ? 35 : second.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (!(o instanceof Pair)) {
                return false;
            }

            Pair<?, ?> p = (Pair<?, ?>) o;

            boolean ef = first == null ? p.first == null : first.equals(p.first);
            boolean es = second == null ? p.second == null : second.equals(p.second);

            return ef && es;
        }

        @Override
        public String toString() {
            return "first='" + first.toString() + "', second='" + second.toString() + "'";
        }
    }

    private final Map<Integer, Pair<ThriftCallFuture, ListenableScheduledFuture>> callbacks = Maps.newHashMap();

    private final AtomicInteger seqId = new AtomicInteger();

    private final ListeningScheduledExecutorService scheduller;

    public AsyncRegister(ListeningScheduledExecutorService scheduller) {
        this.scheduller = scheduller;
    }

    public synchronized ThriftCallFuture pop(int seqId) {
        final Pair<ThriftCallFuture, ListenableScheduledFuture> p = callbacks.remove(seqId);

        if (p == null) {
            return null;
        }

        if (p.second != null) {
            p.second.cancel(false);
        }

        return p.first.isDone() ? null : p.first;
    }

    public synchronized List<ThriftCallFuture> popAll() {
        final List<ThriftCallFuture> ret = Lists.newArrayList();

        for (Pair<ThriftCallFuture, ListenableScheduledFuture> p : callbacks.values()) {

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

    public synchronized void put(final int seqId, ThriftCallFuture ii, final long tmMs) {
        callbacks.put(seqId, new Pair<ThriftCallFuture, ListenableScheduledFuture>(ii, scheduller.schedule(new Runnable() {

            @Override
            public void run() {

                final Pair<ThriftCallFuture, ListenableScheduledFuture> p;

                synchronized (AsyncRegister.this) {
                    p = callbacks.remove(seqId);
                }

                p.first.setException(new TTransportException(TTransportException.TIMED_OUT, "TIMED_OUT"));

            }
        }, tmMs, TimeUnit.MILLISECONDS)));
    }

    public synchronized void put(int seqId, ThriftCallFuture ii) {
        callbacks.put(seqId, new Pair<ThriftCallFuture, ListenableScheduledFuture>(ii, null));
    }

    public int nextSeqId() {
        return seqId.incrementAndGet();
    }

}
