package org.everthrift.cassandra.scheduling.context;

public interface TriggerContextAccessorFactory {

    TriggerContextAccessor get(String taskName);

}
