package org.everthrift.clustering.thrift;

import com.google.common.collect.Maps;
import org.apache.thrift.TBase;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ServiceAsyncIfaceProxy implements InvocationHandler {

    static final Logger log = LoggerFactory.getLogger(ServiceAsyncIfaceProxy.class);

    final static Pattern ifacePattern = Pattern.compile("^([^\\.]+\\.)+([^\\.]+)\\.AsyncIface$");

    final String serviceIfaceName;

    final String serviceName;

    final InvocationCallback callback;

    final Map<String, ThriftMeta> methods = Maps.newHashMap();

    private static class ThriftMeta {
        final Constructor<? extends TBase> args;

        final Constructor<? extends TBase> result;

        public ThriftMeta(Constructor<? extends TBase> args, Constructor<? extends TBase> result) {
            super();
            this.args = args;
            this.result = result;
        }
    }

    /**
     * @param serviceIface thrift интерфейс
     * @param callback     после вызова любого метода сервиса будет вызван
     *                     callback.set
     */
    public ServiceAsyncIfaceProxy(Class serviceIface, InvocationCallback callback) {

        serviceIfaceName = serviceIface.getCanonicalName();

        final Matcher m = ifacePattern.matcher(serviceIfaceName);
        if (m.matches()) {
            serviceName = m.group(2);
        } else {
            throw new RuntimeException("Unknown service name");
        }

        this.callback = callback;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        final ThriftMeta args_result = getThriftMeta(method);

        final AsyncMethodCallback ac = (AsyncMethodCallback) args[args.length - 1];
        final Object methodArgs[] = new Object[args.length - 1];
        System.arraycopy(args, 0, methodArgs, 0, args.length - 1);

        final TBase _args = args_result.args.newInstance(methodArgs);

        // log.info("service={}, method={}, _args={}, _result={}", new
        // Object[]{serviceName, method.getName(), _args, _result});

        try {
            return callback.call(new InvocationInfo(serviceName, method.getName(), _args, args_result.result, ac));
        } catch (NullResult e) {
            final Class rt = method.getReturnType();
            if (rt == Boolean.TYPE) {
                return false;
            } else if (rt == Character.TYPE) {
                return ' ';
            } else if (rt == Byte.TYPE) {
                return (byte) 0;
            } else if (rt == Short.TYPE) {
                return (short) 0;
            } else if (rt == Integer.TYPE) {
                return 0;
            } else if (rt == Long.TYPE) {
                return (long) 0;
            } else if (rt == Float.TYPE) {
                return 0f;
            } else if (rt == Double.TYPE) {
                return (double) 0;
            } else {
                return null;
            }
        }
    }

    private synchronized ThriftMeta getThriftMeta(Method method) throws NoSuchMethodException, SecurityException, ClassNotFoundException {

        ThriftMeta args_result = methods.get(method.getName());

        if (args_result == null) {
            args_result = buildThriftMeta(method);
            methods.put(method.getName(), args_result);
        }

        return args_result;
    }

    private ThriftMeta buildThriftMeta(Method method) throws NoSuchMethodException, SecurityException, ClassNotFoundException {

        final String methodName = method.getName();

        final String argsClassName = serviceIfaceName.replace(".AsyncIface", "$" + methodName + "_args");
        final Class<? extends TBase> argsClass = (Class) Class.forName(argsClassName);

        final String resultClassName = serviceIfaceName.replace(".AsyncIface", "$" + methodName + "_result");
        final Class<? extends TBase> resultClass = (Class) Class.forName(resultClassName);

        final Class<?> methodParams[] = method.getParameterTypes();
        final Class<?> argTypes[] = new Class<?>[methodParams.length - 1];
        System.arraycopy(methodParams, 0, argTypes, 0, methodParams.length - 1);

        final Constructor<? extends TBase> argsConstructor = argsClass.getConstructor(argTypes);

        return new ThriftMeta(argsConstructor, resultClass.getConstructor());
    }

}
