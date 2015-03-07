package com.knockchat.appserver.controller;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessageHeaders;

import com.google.common.collect.Maps;

public class MessageWrapper implements Serializable{

	private static final long serialVersionUID = 1L;
	
	/* attributes */
	public static String SESSION_ID = "SESSION_ID";
	public static String WS_CONTENT_TYPE = "WS_CONTENT_TYPE";
	public static String OUT_CHANNEL = "OUT_CHANNEL";
	public static String MESSAGE_HEADERS = "MESSAGE_HEADERS";
	public static String HTTP_REQUEST_PARAMS = "HTTP_REQUEST_PARAMS";
			
	private TTransport tTransport;
	private Map<String, Object> attributes;
	
	public static enum WebsocketContentType{
		BINARY,
		TEXT
	}
			
	public MessageWrapper(TTransport tTransport) {
		super();
		this.tTransport = tTransport;
		this.attributes = Maps.newHashMap();
	}
	
	@Override
	public String toString() {
		return "MessageWrapper [tTransport=" + tTransport + ", attributes=" + attributes + "]";
	}
	
	public MessageWrapper copySerializeableAttributes(MessageWrapper old){		
		for (Entry<String, Object> e : old.attributes.entrySet()){
			if (e.getValue() instanceof Serializable)
				attributes.put(e.getKey(), e.getValue());
		}		
		return this;
	}
	
	public MessageWrapper copyAttributes(MessageWrapper old){
		this.attributes.putAll(old.attributes);
		return this;
	}	
	
	public MessageWrapper setSessionId(String sessionId){
		attributes.put(SESSION_ID, sessionId);
		return this;
	}
	
	public String getSessionId(){
		return (String)attributes.get(SESSION_ID);
	}
	
	public MessageWrapper setWebsocketContentType(WebsocketContentType websocketContentType){
		attributes.put(WS_CONTENT_TYPE, websocketContentType);
		return this;
	}
	
	public WebsocketContentType getWebsocketContentType(){
		return (WebsocketContentType)attributes.get(WS_CONTENT_TYPE); 
	}
	
	public MessageWrapper setOutChannel(MessageChannel outChannel){
		attributes.put(OUT_CHANNEL, outChannel);
		return this;
	}

	public MessageChannel getOutChannel(){
		return (MessageChannel)attributes.get(OUT_CHANNEL);
	}
	
	public MessageWrapper setMessageHeaders(MessageHeaders messageHeaders){
		attributes.put(MESSAGE_HEADERS, messageHeaders);
		return this;
	}
	
	public MessageHeaders getMessageHeaders(){
		return (MessageHeaders)attributes.get(MESSAGE_HEADERS);
	}
	
	public MessageWrapper toSerializable(){
		return new MessageWrapper(tTransport).copySerializeableAttributes(this);
	}
	
	public TTransport getTTransport(){
		return tTransport;
	}
	
	public Map<String, String[]> getHttpRequestParams(){
		return (Map)attributes.get(HTTP_REQUEST_PARAMS);
	}
	
	public MessageWrapper setHttpRequestParams(Map<String, String[]> params){
		attributes.put(HTTP_REQUEST_PARAMS, params);
		return this;
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		
		if (tTransport == null){
			oos.writeObject(null);	
		}else if (tTransport instanceof TMemoryBuffer){
			oos.writeObject(((TMemoryBuffer)tTransport).toByteArray());
		}else{
			throw new NotSerializableException(tTransport.getClass().getCanonicalName());
		}
		
		oos.writeObject(attributes);
	}
	
	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		byte[] tt = (byte[])ois.readObject();
		if (tt == null)
			tTransport = null;
		else			
			tTransport = new TMemoryInputTransport(tt);
		
		attributes = (Map)ois.readObject();
	}
}
