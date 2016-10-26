package org.everthrift.cassandra.scheduling.context;

import org.everthrift.cassandra.scheduling.DuplicatedTaskException;

import java.io.Serializable;
import java.util.List;

public interface TriggerContextAccessorFactory {

    TriggerContextAccessor get(String taskName, boolean isDynamic);

    TriggerContextAccessor createDynamic(String taskName, long period, long lastScheduledExecutionTime, String beanName, Serializable arg) throws DuplicatedTaskException;

    List<String> getAllDynamic();

}
