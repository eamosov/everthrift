package org.everthrift.cassandra.scheduling;

public class ContextAccessError extends Exception {

    private static final long serialVersionUID = 1L;

    public ContextAccessError() {
        super();
    }

    public ContextAccessError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ContextAccessError(String message, Throwable cause) {
        super(message, cause);
    }

    public ContextAccessError(String message) {
        super(message);
    }

    public ContextAccessError(Throwable cause) {
        super(cause);
    }

}
