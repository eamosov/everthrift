package org.everthrift.appserver.scheduling.zoo;

import java.util.Date;

/**
 * Created by fluder on 06/09/17.
 */
public class ZooData {
    public Date lastScheduledExecutionTime;
    public Date lastActualExecutionTime;
    public Date lastCompletionTime;

    public long period;
    public String beanName;
    public byte[] arg;
    public boolean dynamic;
    boolean cancelled;

    public boolean isDynamic() {
        return dynamic;
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
