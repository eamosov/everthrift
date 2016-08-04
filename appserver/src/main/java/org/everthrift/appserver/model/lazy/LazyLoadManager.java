package org.everthrift.appserver.model.lazy;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class LazyLoadManager {

    private static final Logger log = LoggerFactory.getLogger(LazyLoadManager.class);

    public static int MAX_LOAD_ITERATIONS = 5;

    public static String SCENARIO_DEFAULT = "default";
    public static String SCENARIO_ADMIN = "admin";
    public static String SCENARIO_JSON = "json";
    
    final RegistryImpl registry = new RegistryImpl();

    private static final Executor loadExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r);
            t.setName("LazyLoader-"+ threadNumber.getAndIncrement());
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    });

    public static void nextLap(SettableFuture<Integer> result, AtomicInteger lap, AtomicInteger nAllLoaded, int maxIterations, Object o, Registry r, WalkerIF walker){

        log.debug("Starting load iteration: {}", lap);
        final long st = System.nanoTime();

        walker.apply(o);

        Futures.addCallback(r.load(), new FutureCallback<Integer>(){

            @Override
            public void onSuccess(Integer nLoaded) {

                final long end = System.nanoTime();
                if (log.isDebugEnabled())
                    log.debug("Iteration {} finished. {} entities loaded with {} mcs", new Object[]{lap, nLoaded, (end -st)/1000});

                final int _lap = lap.incrementAndGet();
                final int _nAllLoaded = nAllLoaded.addAndGet(nLoaded);

                if (nLoaded > 0 && _lap < maxIterations)
                    nextLap(result, lap, nAllLoaded, maxIterations, o, r, walker);
                else
                    result.set(_nAllLoaded);
            }

            @Override
            public void onFailure(Throwable t) {
                result.setException(t);
            }}, loadExecutor);

    }

    public static ListenableFuture<Integer> load(int maxIterations, Object o, Registry r, WalkerIF walker){

        log.debug("load, maxIterations={}, o.class={}", maxIterations, o.getClass().getSimpleName());

        final AtomicInteger nAllLoaded = new AtomicInteger(0);
        final AtomicInteger lap  = new AtomicInteger(0);
        final SettableFuture<Integer> result = SettableFuture.create();

        nextLap(result, lap, nAllLoaded, maxIterations, o, r, walker);

        return result;
    }

    public <T> ListenableFuture<T> load(final String scenario, int maxIterations, final T o){
        return load(scenario, maxIterations, o, new Object[]{});
    }

    public <T> ListenableFuture<T> load(final String scenario, int maxIterations, final T o, Object ... args){

        if (o == null ||
                (o instanceof Collection && ((Collection)o).isEmpty()) ||
                (o instanceof Map && ((Map)o).isEmpty()))
            return Futures.immediateFuture(o);

        registry.setArgs(args);
        final WalkerIF walker = new RecursiveWalker(registry, scenario);
        return Futures.transform(load(maxIterations, o, registry, walker), (Integer nLoadAdd) -> o);
    }

    public <T> ListenableFuture<T> load(final String scenario, final T o){
        return load(scenario, MAX_LOAD_ITERATIONS, o);
    }

    public <T> ListenableFuture<T> loadForJson(final T o){
        return load(LazyLoadManager.SCENARIO_JSON, o);
    }

}
