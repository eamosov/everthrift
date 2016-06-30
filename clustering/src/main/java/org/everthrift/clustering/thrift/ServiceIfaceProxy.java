package org.everthrift.clustering.thrift;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ServiceIfaceProxy implements InvocationHandler{

    static final Logger log = LoggerFactory.getLogger(ServiceIfaceProxy.class);

    final static Pattern ifacePattern = Pattern.compile("^([^\\.]+\\.)+([^\\.]+)\\.Iface$");
    final String serviceIfaceName;
    protected final String serviceName;
    final InvocationCallback callback;

    final Map<String, ThriftMeta> methods = Maps.newHashMap();

    protected static class ThriftMeta{
        public final Constructor<? extends TBase> args;
        public final Constructor<? extends TBase> result;

        public ThriftMeta(Constructor<? extends TBase> args, Constructor<? extends TBase> result) {
            super();
            this.args = args;
            this.result = result;
        }
    }

    /**
     *
     * @param serviceIface   thrift интерфейс
     * @param callback 		после вызова любого метода сервиса будет вызван callback.set
     */
    public ServiceIfaceProxy(Class serviceIface, InvocationCallback callback){

        serviceIfaceName =  serviceIface.getCanonicalName();

        final Matcher m = ifacePattern.matcher(serviceIfaceName);
        if (m.matches()){
            serviceName = m.group(2);
        }else{
            throw new RuntimeException("Unknown service name");
        }

        this.callback = callback;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        final ThriftMeta args_result = getThriftMeta(method);

        final TBase _args = args_result.args.newInstance(args);

        //log.info("service={}, method={}, _args={}, _result={}", new Object[]{serviceName, method.getName(), _args, _result});

        try{
            return callback.call(new InvocationInfo(serviceName, method.getName(), _args, args_result.result, null));
        }catch (NullResult e){
            final Class rt = method.getReturnType();
            if (rt == Boolean.TYPE){
                return false;
            }else if (rt == Character.TYPE){
                return ' ';
            }else if (rt == Byte.TYPE){
                return (byte)0;
            }else if (rt == Short.TYPE){
                return (short)0;
            }else if (rt == Integer.TYPE){
                return 0;
            }else if (rt == Long.TYPE){
                return (long)0;
            }else if (rt == Float.TYPE){
                return 0f;
            }else if (rt == Double.TYPE){
                return (double)0;
            }else {
                return null;
            }
        }
    }

    protected synchronized ThriftMeta getThriftMeta(Method method) throws NoSuchMethodException, SecurityException, ClassNotFoundException{

        ThriftMeta args_result = methods.get(method.getName());

        if (args_result == null){
            args_result = buildThriftMeta(method);
            methods.put(method.getName(), args_result);
        }

        return args_result;
    }

    private ThriftMeta buildThriftMeta(Method method) throws NoSuchMethodException, SecurityException, ClassNotFoundException{

        final String methodName = method.getName();

        final String argsClassName = serviceIfaceName.replace(".Iface", "$" + methodName + "_args");
        final Class<? extends TBase> argsClass = (Class)Class.forName(argsClassName);

        final String resultClassName = serviceIfaceName.replace(".Iface", "$" + methodName + "_result");
        final Class<? extends TBase> resultClass = (Class)Class.forName(resultClassName);

        final Constructor<? extends TBase> argsConstructor =  argsClass.getConstructor(method.getParameterTypes());

        return new ThriftMeta(argsConstructor, resultClass.getConstructor());
    }

}
