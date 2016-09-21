package org.everthrift.appserver.model.lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class LazyLoadManager {

    private static final Logger log = LoggerFactory.getLogger(LazyLoadManager.class);

    public static final int MAX_LOAD_ITERATIONS = 5;

    public static final String SCENARIO_DEFAULT = "default";

    public static final String SCENARIO_ADMIN = "admin";

    public static final String SCENARIO_JSON = "json";

    final RegistryImpl registry = new RegistryImpl();

    private static final Executor loadExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r);
            t.setName("LazyLoader-" + threadNumber.getAndIncrement());
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    });

    public static void nextLap(CompletableFuture<Integer> result, AtomicInteger lap, AtomicInteger nAllLoaded, int maxIterations, Object o,
                               Registry r, WalkerIF walker) {

        log.debug("Starting load iteration: {}", lap);
        final long st = System.nanoTime();

        walker.apply(o);

        r.load().handleAsync((nLoaded, t) -> {

            if (t != null){
                result.completeExceptionally(t);
            }else{
                final long end = System.nanoTime();
                if (log.isDebugEnabled()) {
                    log.debug("Iteration {} finished. {} entities loaded with {} mcs", new Object[]{lap, nLoaded, (end - st) / 1000});
                }

                final int _lap = lap.incrementAndGet();
                final int _nAllLoaded = nAllLoaded.addAndGet(nLoaded);

                if (nLoaded > 0 && _lap < maxIterations) {
                    nextLap(result, lap, nAllLoaded, maxIterations, o, r, walker);
                } else {
                    result.complete(_nAllLoaded);
                }
            }

            return null;
        } , loadExecutor);
    }

    public static CompletableFuture<Integer> load(int maxIterations, Object o, Registry r, WalkerIF walker) {

        log.debug("load, maxIterations={}, o.class={}", maxIterations, o.getClass().getSimpleName());

        final AtomicInteger nAllLoaded = new AtomicInteger(0);
        final AtomicInteger lap = new AtomicInteger(0);
        final CompletableFuture<Integer> result = new CompletableFuture();

        nextLap(result, lap, nAllLoaded, maxIterations, o, r, walker);

        return result;
    }

    public <T> CompletableFuture<T> load(final String scenario, int maxIterations, final T o) {
        return load(scenario, maxIterations, o, new Object[]{});
    }

    public <T> CompletableFuture<T> load(final String scenario, int maxIterations, final T o, Object... args) {

        if (o == null || (o instanceof Collection && ((Collection) o).isEmpty()) || (o instanceof Map && ((Map) o).isEmpty())) {
            return CompletableFuture.completedFuture(o);
        }

        registry.setArgs(args);
        final WalkerIF walker = new RecursiveWalker(registry, scenario);
        return load(maxIterations, o, registry, walker).thenApply(nLoadAdd -> o);
    }

    public <T> CompletableFuture<T> load(final String scenario, final T o) {
        return load(scenario, MAX_LOAD_ITERATIONS, o);
    }

    public <T> CompletableFuture<T> loadForJson(final T o) {
        return load(LazyLoadManager.SCENARIO_JSON, o);
    }

}
