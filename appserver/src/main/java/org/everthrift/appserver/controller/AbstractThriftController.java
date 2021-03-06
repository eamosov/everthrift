package org.everthrift.appserver.controller;

import com.google.common.base.Function;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.lazy.LazyLoadManager;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.thrift.TFunction;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.everthrift.utils.ExecutionStats;
import org.everthrift.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public abstract class AbstractThriftController<ArgsType extends TBase, ResultType> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected ArgsType args;

    protected LogEntry logEntry;

    protected int seqId;

    protected ThriftClient thriftClient;

    protected boolean loadLazyRelations = true;

    protected ThriftProtocolSupportIF<ArgsType> tps;

    private long startNanos;

    private long endNanos;

    protected boolean noProfile = false;

    protected Class<? extends Annotation> registryAnn;

    protected boolean allowAsyncAnswer;

    protected String serviceName;

    protected String methodName;

    protected ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry;

    @NotNull
    protected LazyLoadManager lazyLoadManager = new LazyLoadManager();

    //    @Autowired
    //    @Qualifier("listeningCallerRunsBoundQueueExecutor")
    private Executor executor;

    /**
     * Флаг, показывающий был лио отправлен какой-либо ответ (результат или
     * исключение)
     */
    private boolean resultSent = false;

    public static final ConcurrentHashMap<String, ExecutionStats> rpcControllesStats = new ConcurrentHashMap<String, ExecutionStats>();

    public abstract void setup(ArgsType args);

    @NotNull
    public String ctrlLog() {
        return "";
    }

    public void setup(ArgsType args,
                      ThriftProtocolSupportIF tps,
                      LogEntry logEntry,
                      int seqId,
                      ThriftClient thriftClient,
                      Class<? extends Annotation> registryAnn,
                      boolean allowAsyncAnswer,
                      String serviceName,
                      String methodName,
                      Executor executor,
                      ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry) {

        this.args = args;
        this.logEntry = logEntry;
        this.seqId = seqId;
        this.thriftClient = thriftClient;
        this.registryAnn = registryAnn;
        this.tps = tps;
        this.startNanos = System.nanoTime();
        this.allowAsyncAnswer = allowAsyncAnswer;
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.executor = executor;
        this.thriftMethodEntry = thriftMethodEntry;

        rpcControllesStats.putIfAbsent(this.getClass().getSimpleName(), new ExecutionStats());
    }

    @Nullable
    protected abstract ResultType handle() throws TException;

    /**
     * @param args
     * @return TApplicationException || TException || ResultType
     */
    protected final Object handle(ArgsType args) {

        if (log.isDebugEnabled()) {
            log.debug("args:{}, attributes:{}", args, tps.getAttributes());
        }

        try {
            setup(args);
            final CompletableFuture<ResultType> resultFuture = loadLazyRelations(handle());

            if (resultFuture.isDone() || !allowAsyncAnswer) {
                ResultType result;
                try {
                    result = filterOutput(resultFuture.get());
                } catch (@NotNull InterruptedException | ExecutionException e) {
                    setResultSentFlag();
                    setEndNanos(System.nanoTime());
                    log.error("Uncought exception", e);
                    return e;
                }
                setResultSentFlag();
                setEndNanos(System.nanoTime());
                return result;
            } else {
                resultFuture.whenCompleteAsync((_answer, t) -> {
                    if (t != null) {
                        setResultSentFlag();
                        setEndNanos(System.nanoTime());
                        log.error("Uncought exception", t);
                        sendAnswerOrException(t);
                    } else {
                        final ResultType result = filterOutput(_answer);
                        setResultSentFlag();
                        setEndNanos(System.nanoTime());
                        sendAnswerOrException(result);
                    }
                }, executor);

                return waitForAnswer();
            }
        } catch (TException e) {
            return e;
        }
    }

    @NotNull
    protected ResultType waitForAnswer() {
        throw new AsyncAnswer();
    }

    private static class ExUtil {
        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void throwException(Throwable exception, Object dummy) throws T {
            throw (T) exception;
        }

        public static void throwException(Throwable exception) {
            ExUtil.<RuntimeException>throwException(exception, null);
        }
    }

    @Nullable
    protected <F, T> Function<F, T> unchecked(@NotNull TFunction<F, T> f) {
        return new Function<F, T>() {

            @Override
            public T apply(F input) {
                try {
                    return f.apply(input);
                } catch (TException e) {
                    ExUtil.throwException(e);
                    return null;
                }
            }
        };
    }

    protected ResultType waitForAnswer(@NotNull CompletableFuture<? extends ResultType> lf) throws TException {

        if (!allowAsyncAnswer) {
            try {
                return lf.get();
            } catch (InterruptedException e) {
                throw new TApplicationException(TApplicationException.INTERNAL_ERROR, e.getMessage());
            } catch (ExecutionException e) {
                final Throwable t = e.getCause();

                if (t instanceof TException) {
                    throw (TException) t;
                } else if (t.getCause() instanceof TException) {
                    throw (TException) t.getCause();
                } else {
                    log.error("Exception", e);
                    throw new TApplicationException(t.getMessage());
                }
            }

        } else {
            lf.whenCompleteAsync((result, t) -> {
                if (t != null) {
                    if (t instanceof TException) {
                        sendException((TException) t);
                    } else if (t.getCause() instanceof TException) {
                        sendException((TException) t.getCause());
                    } else {
                        log.error("Exception", t);
                        sendException(new TApplicationException(t.getMessage()));
                    }
                } else {
                    sendAnswer(result);
                }
            }, executor);

            throw new AsyncAnswer();
        }
    }

    protected final synchronized void sendException(TException answer) {
        setResultSentFlag();
        setEndNanos(System.nanoTime());
        sendAnswerOrException(answer);
    }

    protected final synchronized void sendAnswer(ResultType answer) {
        setResultSentFlag();

        loadLazyRelations(answer).handleAsync((_answer, t) -> {
            if (t != null) {
                log.error("Uncought exception", t);
                setEndNanos(System.nanoTime());
                sendAnswerOrException(new TApplicationException(TApplicationException.INTERNAL_ERROR, t.getMessage()));
                return null;
            }
            final ResultType result = filterOutput(_answer);
            setEndNanos(System.nanoTime());
            sendAnswerOrException(result);
            return null;
        }, executor);
    }

    private void sendAnswerOrException(Object answer) {
        tps.serializeReplyAsync(answer, this);
    }

    /**
     * Установить флаг отправленного запроса и вычислить время выполнения
     * запроса
     */
    private synchronized void setResultSentFlag() {
        if (resultSent) {
            throw new IllegalStateException(this.getClass().getSimpleName() + ": Result already sent");
        }

        resultSent = true;

        if (!noProfile) {
            final ExecutionStats es = rpcControllesStats.get(this.getClass().getSimpleName());
            if (es != null) {
                es.update(getExecutionMcs());
            }
        }

    }

    public long getEndNanos() {
        return endNanos;
    }

    private synchronized void setEndNanos(long endNanos) {
        this.endNanos = endNanos;
    }

    public long getStartNanos() {
        return startNanos;
    }

    public synchronized void setStartNanos(long startNanos) {
        this.startNanos = startNanos;
    }

    public long getExecutionMcs() {
        return (endNanos - startNanos) / 1000;
    }

    public long getWarnExecutionMcsLimit() {
        return 100000;
    }

    public static void resetExecutionLog() {
        rpcControllesStats.clear();
    }

    public static synchronized String getExecutionLog() {
        final ArrayList<Pair<String, ExecutionStats>> list = new ArrayList<Pair<String, ExecutionStats>>(rpcControllesStats
                                                                                                             .size());

        final Iterator<Entry<String, ExecutionStats>> it = rpcControllesStats.entrySet().iterator();

        while (it.hasNext()) {
            final Entry<String, ExecutionStats> e = it.next();
            final ExecutionStats stats;
            synchronized (e.getValue()) {
                stats = new ExecutionStats(e.getValue());
            }
            list.add(new Pair<String, ExecutionStats>(e.getKey(), stats));

        }

        Collections.sort(list, new Comparator<Pair<String, ExecutionStats>>() {

            @Override
            public int compare(@NotNull Pair<String, ExecutionStats> o1, @NotNull Pair<String, ExecutionStats> o2) {
                return Long.signum(o2.second.getSummaryTime() - o1.second.getSummaryTime());
            }
        });

        return ExecutionStats.getLogString(list);
    }

    // Обработчик после lasyload и перед отправкой данных клиенту
    protected ResultType filterOutput(ResultType result) {
        return result;
    }

    @NotNull
    protected CompletableFuture<ResultType> loadLazyRelations(ResultType result) {
        return loadLazyRelations ? lazyLoadManager.load(LazyLoadManager.SCENARIO_DEFAULT, result) : CompletableFuture.completedFuture(result);
    }

    @NotNull
    protected Map<String, String[]> getHttpRequestParams() {
        return ((Map<String, String[]>) tps.getAttributes().get(MessageWrapper.HTTP_REQUEST_PARAMS));
    }

    @NotNull
    protected Map<String, String> getHttpHeaders() {
        return (Map<String, String>) tps.getAttributes().get(MessageWrapper.HTTP_HEADERS);
    }

    public ThriftServicesDiscovery.ThriftMethodEntry getThriftMethodEntry() {
        return thriftMethodEntry;
    }
}
