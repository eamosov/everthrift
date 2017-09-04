package org.everthrift.appserver.scheduling;

import java.io.Serializable;

/**
 * Created by fluder on 25.10.16.
 */
public interface DynamicTaskRunnable {
    void runTask(String taskName, Serializable arg);
}
