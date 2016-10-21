package org.everthrift.cassandra.scheduling.context;

import org.springframework.scheduling.TriggerContext;

import java.util.Date;

public interface SettableTriggerContext extends TriggerContext {

    void setLastScheduledExecutionTime(Date lastScheduledExecutionTime);

    void setLastActualExecutionTime(Date lastActualExecutionTime);

    void setLastCompletionTime(Date lastCompletionTime);

}