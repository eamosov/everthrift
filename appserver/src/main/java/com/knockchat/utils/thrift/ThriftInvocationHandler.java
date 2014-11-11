package com.knockchat.utils.thrift;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.knockchat.appserver.model.LazyLoadManager;
import com.knockchat.appserver.model.LazyLoadManager.LoadList;

public class ThriftInvocationHandler implements InvocationHandler{
	
	private static final Logger log = LoggerFactory.getLogger(ThriftInvocationHandler.class);

	final static Pattern ifacePattern = Pattern.compile("^([^\\.]+\\.)+([^\\.]+)\\.Iface$");
	final String serviceIfaceName;
	final String serviceName;
	final SettableFuture<InvocationInfo> callback;
	
	final Map<String, ThriftMeta> methods = Maps.newHashMap();
	
	public static class InvocationInfo<T> extends AbstractFuture<T>{
		public final String methodName;
		public final TBase args;
		public final Constructor<? extends TBase> resultInit;
		
		private int seqId;

		public InvocationInfo(String methodName, TBase args, Constructor<? extends TBase> resultInit) {
			super();
			this.methodName = methodName;
			this.args = args;
			this.resultInit = resultInit;
			
			lazyLoadArgs();
		}

		public InvocationInfo(String serviceName, String methodName, TBase args, Constructor<? extends TBase> resultInit) {
			super();
			this.methodName = serviceName + ":" + methodName;
			this.args = args;
			this.resultInit = resultInit;
			
			lazyLoadArgs();
		}
		
		private void lazyLoadArgs(){
			final LoadList ll = LazyLoadManager.get();
			ll.load(this.args);
		}

		/**
		 * 
		 * @param seqId
		 * @param protocolFactory
		 * @return
		 * 
		 * Transform to array:
		 * 
		 * payload.array(), payload.position(), payload.limit() - payload.position()
		 */
		public TMemoryBuffer buildCall(int seqId, TProtocolFactory protocolFactory){
			this.seqId = seqId;
			
			final TMemoryBuffer outT = new TMemoryBuffer(1024);
				
			final TProtocol outProtocol = protocolFactory.getProtocol(outT);
			
			try {
				outProtocol.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, seqId));
			    args.write(outProtocol);
			    outProtocol.writeMessageEnd();
			    outProtocol.getTransport().flush();
						
				return outT;				
			} catch (TException e) {
				throw new RuntimeException(e);
			}							
		}
		
		public T setReply(byte[] data, TProtocolFactory protocolFactory) throws TException{
			return setReply(data, 0, data.length, protocolFactory);
		}
		
		public T setReply(byte[] data, int offset, int length, TProtocolFactory protocolFactory) throws TException{
			
			try(
					TTransport inT = new TMemoryInputTransport(data, offset, length);
				){
				return setReply(inT, protocolFactory);
			}
		}
		
		public T setReply(TTransport inT, TProtocolFactory protocolFactory) throws TException{
			try{
				final T ret = (T)this.parseReply(inT, protocolFactory);
				super.set(ret);
				return ret;
			}catch(TException e){
				super.setException(e);
				throw e;
			}
		}
		
		public void setException(TException e){
			super.setException(e);
		}
				
		private Object parseReply(TTransport inT, TProtocolFactory protocolFactory) throws TException{
			final TProtocol inProtocol = protocolFactory.getProtocol(inT);
			
			TMessage msg = inProtocol.readMessageBegin();
			if (msg.type == TMessageType.EXCEPTION) {
				TApplicationException x = TApplicationException.read(inProtocol);
				inProtocol.readMessageEnd();
				throw x;
			}
			
			if (msg.type != TMessageType.REPLY){
				throw new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE,  this.methodName + " failed: invalid msg type"); 
			}				
			
			if (msg.seqid != seqId) {
				throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, methodName + " failed: out of sequence response");
			}
			
			if (!msg.name.equals(this.methodName)){
				throw new TApplicationException(TApplicationException.WRONG_METHOD_NAME, methodName + " failed: invalid method name '" + msg.name + "' in reply. Need '" + this.methodName + "'");
			}
			
			final TBase result;
			try {
				result = resultInit.newInstance();
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
			
			result.read(inProtocol);
			inProtocol.readMessageEnd();
			
			log.debug("result: {}", result);
			
			Object o = null;
			int i=1;
			do{//Пытаемся найти exception				
				final TFieldIdEnum f = result.fieldForId(i++);
				if (f==null)
					break;
				
				o = result.getFieldValue(f);
				if (o!=null)
					break;
			}while(o==null);
			
			if (o==null){//Пробуем прочитать success
				final TFieldIdEnum f = result.fieldForId(0);
				if (f!=null)
					o = result.getFieldValue(f);
			}
			
			if (o == null){
				return null;
			}
			
			if (o instanceof RuntimeException)
				throw (RuntimeException)o;
			else if (o instanceof TException)
				throw (TException)o;
			
			return o;											
		}								
	}
	
	@SuppressWarnings("rawtypes")
	private static class ThriftMeta{		
		final Constructor<? extends TBase> args;
		final Constructor<? extends TBase> result;
		
		@SuppressWarnings("rawtypes")
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
	public ThriftInvocationHandler(Class serviceIface, SettableFuture<InvocationInfo> callback){
					
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
		
		final ThriftMeta args_result = getArgsResult(method);
		
		final TBase _args = args_result.args.newInstance(args);
		
		//log.info("service={}, method={}, _args={}, _result={}", new Object[]{serviceName, method.getName(), _args, _result});
		
		callback.set(new InvocationInfo(serviceName, method.getName(), _args, args_result.result));
				
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
		
	private synchronized ThriftMeta getArgsResult(Method method) throws NoSuchMethodException, SecurityException{
		
		ThriftMeta args_result = methods.get(method.getName());
		
		if (args_result == null){
			args_result = buildArgsResult(method);
			methods.put(method.getName(), args_result);
		}
		
		return args_result;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private ThriftMeta buildArgsResult(Method method) throws NoSuchMethodException, SecurityException{
		
		final String methodName = method.getName();
		
		final String argsClassName = serviceIfaceName.replace(".Iface", "." + methodName + "_args");
		final Class<? extends TBase> argsClass = (Class)ClassUtils.resolveClassName(argsClassName, ClassUtils.getDefaultClassLoader());
		
		final String resultClassName = serviceIfaceName.replace(".Iface", "." + methodName + "_result");
		final Class<? extends TBase> resultClass = (Class)ClassUtils.resolveClassName(resultClassName, ClassUtils.getDefaultClassLoader());
					
		final Constructor<? extends TBase> argsConstructor =  argsClass.getConstructor(method.getParameterTypes());
		
		final String clientClassName = serviceIfaceName.replace(".Iface", ".Client");
		final Class<? extends TServiceClient> clientClass = (Class)ClassUtils.resolveClassName(clientClassName, ClassUtils.getDefaultClassLoader());
		
		return new ThriftMeta(argsConstructor, resultClass.getConstructor());
	}

}
