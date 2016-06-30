package org.everthrift.appserver.controller;

import java.util.List;

public class LogEntry {
    public final String serviceName;
    public final long startMcs;
    public long runMcs;
    public long endMcs;
    public int seqId;

    public LogEntry(String serviceName){
        this.serviceName = serviceName;
        this.startMcs = System.nanoTime() / 1000;
    }

    public LogEntry(String serviceName, long endMcs){
        this(serviceName);
        this.endMcs = endMcs;
        this.runMcs = endMcs;
    }


    @Override
    public String toString(){
        if (endMcs > 0)
            return serviceName + "/" + seqId +"(" + ((runMcs - startMcs) / 1000) + "/" + ((endMcs - runMcs) / 1000) + " ms)";
        else if (runMcs > 0)
            return serviceName + "/" + seqId +"(" + ((runMcs - startMcs) / 1000) + "/n ms)";
        else
            return serviceName + "/" + seqId + "(not finished)";
    }

    static String toString(List<LogEntry> logs){
        synchronized(logs){
            final StringBuilder sb = new StringBuilder();

            LogEntry last = null;
            for (LogEntry e: logs){
                if (last!=null){
                    sb.append(", ");
                    sb.append((e.startMcs - last.startMcs)/1000);
                    sb.append(" ms, ");
                }
                sb.append(e.toString());
                last = e;
            }

            return sb.toString();
        }
    }

}
