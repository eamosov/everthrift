package org.everthrift.appserver.model;

public interface CreatedAtIF {
    void setCreatedAt(long value);

    long getCreatedAt();

    static void setCreatedAt(Object e, long now) {
        if (e instanceof CreatedAtIF && (((CreatedAtIF) e).getCreatedAt() == 0)) {
            ((CreatedAtIF) e).setCreatedAt(now);
        }

        if (e instanceof UpdatedAtIF) {
            ((UpdatedAtIF) e).setUpdatedAt(now);
        }
    }

    static void setCreatedAt(Object e) {
        setCreatedAt(e, System.currentTimeMillis());
    }

}
