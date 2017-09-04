package org.everthrift.appserver.scheduling.context;

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
    void setPeriod(Long period);

    String getBeanName();

    Serializable getArg();

    boolean isCancelled();
    void setCancelled(boolean cancelled);
}