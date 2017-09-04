package org.everthrift.appserver.scheduling.context;

import org.everthrift.appserver.scheduling.DuplicatedTaskException;

import java.io.Serializable;
import java.util.List;

public interface TriggerContextAccessorFactory {

    TriggerContextAccessor get(String taskName, boolean isDynamic);

    TriggerContextAccessor createDynamic(String taskName, long period, long lastScheduledExecutionTime, String beanName, Serializable arg) throws DuplicatedTaskException;

    List<String> getAllDynamic();

}
