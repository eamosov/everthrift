package com.knockchat.node.model;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TBase;
import org.springframework.util.StringUtils;

import com.knockchat.proactor.handlers.Handler;
import com.knockchat.proactor.handlers.Handlers;

public class AwareAdapterFactory {

    public static final String IS_SET_FORMAT = "isSet%s_id";
    public static final String SET_FORMAT = "set%s";
    public static final String GET_FORMAT = "get%s_id";

    public static <K, V> XAwareIF<K, V> getAwareAdapter(TBase o, String fieldName) {
        return new ProxyWrapper<K,V>(o, 
        		String.format(IS_SET_FORMAT, StringUtils.capitalize(fieldName)),
        		String.format(GET_FORMAT, StringUtils.capitalize(fieldName)),
        		String.format(SET_FORMAT, StringUtils.capitalize(fieldName))
        		);
    }

    public static <K, V> XAwareIF<K, V> getAwareAdapter(Object o, String isSetIdMethodName, String getIdMethodName, String setMethodName) {
        return new ProxyWrapper<K,V>(o, isSetIdMethodName, getIdMethodName, setMethodName);
    }

    public static class ProxyWrapper<K, V> implements XAwareIF<K, V> {

        private final Handler getId;
        private final Handler isSetId;
        private final Handler set;

        public ProxyWrapper(Object o, String isSetIdMethodName, String getIdMethodName, String setMethodName) {
            super();
            Lock l = new ReentrantLock();
            this.isSetId = Handlers.getFactory(o.getClass()).get(o, isSetIdMethodName, l);
            this.getId = Handlers.getFactory(o.getClass()).get(o, getIdMethodName, l);
            this.set = Handlers.getFactory(o.getClass()).get(o, setMethodName, l);
        }

        @Override
        public boolean isSetId() {
            return (boolean) isSetId.handle(null);
        }

        @Override
        public void set(V o) {
            set.handle(o);
        }

        @Override
        public K getId() {
            return (K) getId.handle(null);
        }

    }

}
