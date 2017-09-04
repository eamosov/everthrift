package org.everthrift.appserver.scheduling;

/**
 * Created by fluder on 26.10.16.
 */
public class DynamicTaskEvent {

    private final String taskName;

    public DynamicTaskEvent(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskName() {
        return taskName;
    }
}
