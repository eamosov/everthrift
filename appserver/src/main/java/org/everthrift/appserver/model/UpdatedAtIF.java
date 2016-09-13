package org.everthrift.appserver.model;

public interface UpdatedAtIF {
    void setUpdatedAt(long value);

    long getUpdatedAt();

    static void setUpdatedAt(Object e, long now) {
        if (e instanceof UpdatedAtIF) {
            ((UpdatedAtIF) e).setUpdatedAt(now);
        }
    }

    static void setUpdatedAt(Object e) {
        setUpdatedAt(e, System.currentTimeMillis());
    }
}
