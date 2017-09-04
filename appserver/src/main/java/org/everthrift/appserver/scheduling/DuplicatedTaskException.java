package org.everthrift.appserver.scheduling;

/**
 * Created by fluder on 25.10.16.
 */
public class DuplicatedTaskException extends Exception{
    private final String id;

    public DuplicatedTaskException(String id) {
        super("Task with id " + id + " has been allready existed");
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
