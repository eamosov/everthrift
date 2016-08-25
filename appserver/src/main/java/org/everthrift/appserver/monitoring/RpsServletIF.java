package org.everthrift.appserver.monitoring;

public interface RpsServletIF {

    public static enum DsName {

        THRIFT_TCP("th_tcp"),
        THRIFT_HTTP("th_http"),
        THRIFT_JGROUPS("th_jgroups"),
        THRIFT_JMS("th_jms"),
        THRIFT_WS("th_ws");

        public final String dsName;

        DsName(String dsName) {
            this.dsName = dsName;
        }
    }

    void incThrift(DsName dsName);
}
