package org.everthrift.thrift;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.everthrift.utils.ClassUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.RegexPatternTypeFilter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by fluder on 24/05/2018.
 */
public class ThriftServicesDiscovery {

    private static final Logger log = LoggerFactory.getLogger(ThriftServicesDiscovery.class);

    public static ThriftServicesDiscovery INSTANCE;

    public static class ThriftMethodEntry {
        public final String serviceName;
        public final Class serviceIFaceClass;
        public final String methodName;
        public final Method method;

        public final Class<TBase> argsCls;
        public final Constructor<TBase> argsConstructor;

        public final Class<TBase> resultCls;
        public final Method resultSetSuccess;

        public ThriftMethodEntry(String serviceName, Class serviceIFaceClass, String methodName, Class<TBase> argsCls, Class<TBase> resultCls) {
            this.serviceName = serviceName;
            this.serviceIFaceClass = serviceIFaceClass;
            this.methodName = methodName;
            this.method = Arrays.stream(this.serviceIFaceClass.getMethods())
                                .filter(it -> it.getName().equals(methodName))
                                .findFirst()
                                .get();
            this.argsCls = argsCls;
            this.resultCls = resultCls;

            final Class<?> methodParams[] = method.getParameterTypes();

            try {
                argsConstructor = argsCls.getConstructor(methodParams);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }

            resultSetSuccess = Arrays.stream(this.resultCls.getMethods())
                                     .filter(it -> it.getName().equals("setSuccess"))
                                     .findFirst()
                                     .orElse(null);
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getMethodName() {
            return methodName;
        }

        public String getFullMethodName() {
            return serviceName + ":" + methodName;
        }

        public TBase makeArgs() {
            try {
                return argsCls.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @NotNull
        public TBase makeResult(@Nullable Object successOrException) throws TApplicationException {

            try {

                final TBase res = resultCls.newInstance();

                if (successOrException != null) {
                    if (successOrException instanceof TException) {
                        try {
                            final Method setMethod = ClassUtils.getPropertyDescriptors(resultCls)
                                                               .entrySet()
                                                               .stream()
                                                               .filter(e ->
                                                                           e.getValue().getReadMethod() != null
                                                                               && e.getValue().getWriteMethod() != null
                                                                               && e.getValue()
                                                                                   .getReadMethod()
                                                                                   .getReturnType()
                                                                                   .isAssignableFrom(successOrException.getClass()))
                                                               .findFirst().get().getValue().getWriteMethod();
                            setMethod.invoke(res, successOrException);
                        } catch (NoSuchElementException e) {
                            log.error("Coudn't find Exception of type {} in {}",
                                      successOrException.getClass().getCanonicalName(),
                                      resultCls.getCanonicalName());

                            throw new TApplicationException(TApplicationException.INTERNAL_ERROR,
                                                            successOrException.getClass().getSimpleName()
                                                                + ":"
                                                                + ((TException) successOrException).getMessage());
                        }
                    } else {
                        resultSetSuccess.invoke(res, successOrException);
                    }
                }
                return res;
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | NoSuchElementException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        public void serializeCall(int seqId, TBase args, TTransport outT, TProtocolFactory protocolFactory) {

            final TProtocol outProtocol = protocolFactory.getProtocol(outT);

            try {
                outProtocol.writeMessageBegin(new TMessage(serviceName + ":" + methodName, TMessageType.CALL, seqId));
                args.write(outProtocol);
                outProtocol.writeMessageEnd();
                outProtocol.getTransport().flush();
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * @param seqId
         * @param protocolFactory
         * @return Transform to array:
         * <p>
         * payload.array(), payload.position(), payload.limit() - payload.position()
         */
        public TMemoryBuffer serializeCall(int seqId, TBase args, TProtocolFactory protocolFactory) {
            final TMemoryBuffer outT = new TMemoryBuffer(1024);
            serializeCall(seqId, args, outT, protocolFactory);
            return outT;
        }

        @SuppressWarnings("unchecked")
        public Object deserializeReply(int seqId, TTransport inT, TProtocolFactory protocolFactory) throws TException {
            final TProtocol inProtocol = protocolFactory.getProtocol(inT);

            final TMessage msg = inProtocol.readMessageBegin();
            if (msg.type == TMessageType.EXCEPTION) {
                TApplicationException x = new TApplicationException();
                x.read(inProtocol);
                inProtocol.readMessageEnd();
                throw x;
            }

            if (msg.type != TMessageType.REPLY) {
                throw new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE, getFullMethodName() + " failed: invalid msg type");
            }

            if (msg.seqid != seqId) {
                throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, getFullMethodName() + " failed: out of sequence response");
            }

            if (!msg.name.equals(getFullMethodName())) {
                throw new TApplicationException(TApplicationException.WRONG_METHOD_NAME,
                                                getFullMethodName() + " failed: invalid method name '" + msg.name + "' in reply. Need '"
                                                    + getFullMethodName() + "'");
            }

            final TBase result;
            try {
                result = resultCls.newInstance();
            } catch (IllegalArgumentException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            result.read(inProtocol);
            inProtocol.readMessageEnd();

            Object o = null;
            int i = 1;
            do {// Пытаемся найти exception
                final TFieldIdEnum f = result.fieldForId(i++);
                if (f == null) {
                    break;
                }

                o = result.getFieldValue(f);
                if (o != null) {
                    break;
                }
            } while (o == null);

            if (o == null) {// Пробуем прочитать success
                final TFieldIdEnum f = result.fieldForId(0);
                if (f != null) {
                    o = result.getFieldValue(f);
                }
            }

            if (o == null) {
                return null;
            }

            if (o instanceof RuntimeException) {
                throw (RuntimeException) o;
            } else if (o instanceof TException) {
                throw (TException) o;
            }

            return o;
        }

        @Override
        public String toString() {
            return "ThriftMethodEntry{" +
                "serviceName='" + serviceName + '\'' +
                ", serviceIFaceClass=" + serviceIFaceClass +
                ", methodName='" + methodName + '\'' +
                ", method=" + method +
                ", argsCls=" + argsCls +
                ", resultCls=" + resultCls +
                '}';
        }
    }

    private final Map<String, ThriftMethodEntry> thriftMethodEntryMap = new HashMap<>();

    private final static Pattern methodArgsPattern = Pattern.compile("(.+\\.([^.]+))\\$([^$]+)_args$");

    public ThriftServicesDiscovery(String roots) {
        INSTANCE = this;
        scan(Arrays.asList(roots.split(",")));
    }

    public ThriftMethodEntry getByMethod(String fullMethodName) {
        return thriftMethodEntryMap.get(fullMethodName);
    }

    public ThriftMethodEntry getByMethod(Method method) {
        return thriftMethodEntryMap.get(method.getDeclaringClass().getCanonicalName() + ":" + method.getName());
    }

    private void scan(List<String> roots) {

        final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new RegexPatternTypeFilter(methodArgsPattern));

        for (String root : roots) {
            for (BeanDefinition b : scanner.findCandidateComponents(root)) {

                final Matcher m = methodArgsPattern.matcher(b.getBeanClassName());

                if (m.matches()) {

                    try {
                        final ThriftMethodEntry e = new ThriftMethodEntry(m.group(2),
                                                                          Class.class.forName(m.group(1) + "$Iface"),
                                                                          m.group(3),
                                                                          (Class<TBase>) Class.class.forName(m.group(0)),
                                                                          (Class<TBase>) Class.class.forName(m.group(0)
                                                                                                              .replace("_args", "_result")));

                        thriftMethodEntryMap.put(e.serviceName + ":" + e.methodName, e);
                        thriftMethodEntryMap.put(e.method.getDeclaringClass()
                                                         .getCanonicalName() + ":" + e.methodName, e);

                    } catch (ClassNotFoundException e) {
                    }
                }
            }
        }
    }
}
