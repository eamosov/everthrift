package org.everthrift.appserver.controller;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.lazy.LazyLoadManager;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.thrift.TFunction;
import org.everthrift.utils.ExecutionStats;
import org.everthrift.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public abstract class AbstractThriftController<ArgsType extends TBase, ResultType> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected ArgsType args;

    private ThriftControllerInfo info;

    protected LogEntry logEntry;

    protected int seqId;

    protected DataSource ds;

    protected ThriftClient thriftClient;

    protected boolean loadLazyRelations = true;

    protected ThriftProtocolSupportIF tps;

    @Autowired
    protected ApplicationContext context;

    private long startNanos;

    private long endNanos;

    protected boolean noProfile = false;

    protected Class<? extends Annotation> registryAnn;

    protected boolean allowAsyncAnswer;

    protected LazyLoadManager lazyLoadManager = new LazyLoadManager();

    @Autowired
    @Qualifier("listeningCallerRunsBoundQueueExecutor")
    private ListeningExecutorService executor;

    /**
     * Флаг, показывающий был лио отправлен какой-либо ответ (результат или
     * исключение)
     */
    private boolean resultSent = false;

    public static final ConcurrentHashMap<String, ExecutionStats> rpcControllesStats = new ConcurrentHashMap<String, ExecutionStats>();

    public abstract void setup(ArgsType args);

    public String ctrlLog() {
        return "";
    }

    public void setup(ArgsType args, ThriftControllerInfo info, ThriftProtocolSupportIF tps, LogEntry logEntry, int seqId,
                      ThriftClient thriftClient, Class<? extends Annotation> registryAnn, boolean allowAsyncAnswer) {
        this.args = args;
        this.info = info;
        this.logEntry = logEntry;
        this.seqId = seqId;
        this.thriftClient = thriftClient;
        this.registryAnn = registryAnn;
        this.tps = tps;
        this.startNanos = System.nanoTime();
        this.allowAsyncAnswer = allowAsyncAnswer;

        try {
            this.ds = context.getBean(DataSource.class);
        }
        catch (NoSuchBeanDefinitionException e) {
            this.ds = null;
        }

        rpcControllesStats.putIfAbsent(this.getClass().getSimpleName(), new ExecutionStats());
    }

    protected abstract ResultType handle() throws TException;

    /**
     *
     * @param args
     * @return TApplicationException || TException || ResultType
     */
    protected final Object handle(ArgsType args) {

        log.debug("args:{}, attributes:{}", args, tps.getAttributes());

        try {
            setup(args);
            final ListenableFuture<ResultType> resultFuture = loadLazyRelations(handle());

            if (resultFuture.isDone() || allowAsyncAnswer == false) {
                ResultType result;
                try {
                    result = filterOutput(resultFuture.get());
                }
                catch (InterruptedException | ExecutionException e) {
                    setResultSentFlag();
                    setEndNanos(System.nanoTime());
                    log.error("Uncought exception", e);
                    return e;
                }
                setResultSentFlag();
                setEndNanos(System.nanoTime());
                return result;
            } else {
                Futures.addCallback(resultFuture, new FutureCallback<ResultType>() {

                    @Override
                    public void onSuccess(ResultType _answer) {
                        final ResultType result = filterOutput(_answer);
                        setResultSentFlag();
                        setEndNanos(System.nanoTime());
                        sendAnswerOrException(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        setResultSentFlag();
                        setEndNanos(System.nanoTime());
                        log.error("Uncought exception", t);
                        sendAnswerOrException(t);
                    }
                }, executor);

                return waitForAnswer();
            }
        }
        catch (TException e) {
            return e;
        }
    }

    protected ResultType waitForAnswer() {
        throw new AsyncAnswer();
    }

    private static class ExUtil {
        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void throwException(Throwable exception, Object dummy) throws T {
            throw (T) exception;
        }

        public static void throwException(Throwable exception) {
            ExUtil.<RuntimeException> throwException(exception, null);
        }
    }

    protected <F, T> Function<F, T> unchecked(TFunction<F, T> f) {
        return new Function<F, T>() {

            @Override
            public T apply(F input) {
                try {
                    return f.apply(input);
                }
                catch (TException e) {
                    ExUtil.throwException(e);
                    return null;
                }
            }
        };
    }

    protected ResultType waitForAnswer(ListenableFuture<? extends ResultType> lf) throws TException {

        if (!allowAsyncAnswer) {
            try {
                return lf.get();
            }
            catch (InterruptedException e) {
                throw new TApplicationException(TApplicationException.INTERNAL_ERROR, e.getMessage());
            }
            catch (ExecutionException e) {
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
            Futures.addCallback(lf, new FutureCallback<ResultType>() {

                @Override
                public void onSuccess(ResultType result) {
                    sendAnswer(result);
                }

                @Override
                public void onFailure(Throwable t) {

                    if (t instanceof TException) {
                        sendException((TException) t);
                    } else if (t.getCause() instanceof TException) {
                        sendException((TException) t.getCause());
                    } else {
                        log.error("Exception", t);
                        sendException(new TApplicationException(t.getMessage()));
                    }
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

        Futures.addCallback(loadLazyRelations((ResultType) answer), new FutureCallback<ResultType>() {

            @Override
            public void onSuccess(ResultType _answer) {
                final ResultType result = filterOutput(_answer);
                setEndNanos(System.nanoTime());
                sendAnswerOrException(result);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Uncought exception", t);
            }
        }, executor);
    }

    protected void sendAnswerOrException(Object answer) {
        tps.asyncResult(answer, this);
    }

    /**
     * Установить флаг отправленного запроса и вычислить время выполнения
     * запроса
     */
    private synchronized void setResultSentFlag() {
        if (resultSent)
            throw new IllegalStateException(this.getClass().getSimpleName() + ": Result already sent");

        resultSent = true;

        if (!noProfile) {
            final ExecutionStats es = rpcControllesStats.get(this.getClass().getSimpleName());
            if (es != null)
                es.update(getExecutionMcs());
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
        final ArrayList<Pair<String, ExecutionStats>> list = new ArrayList<Pair<String, ExecutionStats>>(rpcControllesStats.size());

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
            public int compare(Pair<String, ExecutionStats> o1, Pair<String, ExecutionStats> o2) {
                return Long.signum(o2.second.getSummaryTime() - o1.second.getSummaryTime());
            }
        });

        return ExecutionStats.getLogString(list);
    }

    // Обработчик после lasyload и перед отправкой данных клиенту
    protected ResultType filterOutput(ResultType result) {
        return result;
    }

    protected ListenableFuture<ResultType> loadLazyRelations(ResultType result) {
        return loadLazyRelations ? lazyLoadManager.load(LazyLoadManager.SCENARIO_DEFAULT, result) : Futures.immediateFuture(result);
    }

    protected Map<String, String[]> getHttpRequestParams() {
        return ((Map<String, String[]>) tps.getAttributes().get(MessageWrapper.HTTP_REQUEST_PARAMS));
    }

    protected Map<String, String> getHttpHeaders() {
        return (Map<String, String>) tps.getAttributes().get(MessageWrapper.HTTP_HEADERS);
    }

    public ThriftControllerInfo getInfo() {
        return info;
    }

}
