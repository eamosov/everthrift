package org.everthrift.cassandra.scheduling.context;

import org.springframework.scheduling.TriggerContext;

import java.io.Serializable;
import java.util.Date;

public interface SettableTriggerContext extends TriggerContext {

    void setLastScheduledExecutionTime(Date lastScheduledExecutionTime);

    void setLastActualExecutionTime(Date lastActualExecutionTime);

    void setLastCompletionTime(Date lastCompletionTime);

    default boolean isDynamic() {
        return getBeanName() != null;
    }

    Long getPeriod();

    String getBeanName();

    Serializable getArg();

    boolean isCancelled();
    void setCancelled(boolean cancelled);
}