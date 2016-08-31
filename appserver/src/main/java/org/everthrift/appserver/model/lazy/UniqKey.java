package org.everthrift.appserver.model.lazy;

/**
 * Created by fluder on 30.08.16.
 */
public class UniqKey {
    final Object entity;

    final Object eq;

    public UniqKey(Object entity, Object eq) {
        super();
        this.entity = entity;
        this.eq = eq;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((entity == null) ? 0 : System.identityHashCode(entity));
        result = prime * result + ((eq == null) ? 0 : eq.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        UniqKey other = (UniqKey) obj;
        if (entity != other.entity) {
            return false;
        }

        if (eq == null) {
            if (other.eq != null) {
                return false;
            }
        } else if (!eq.equals(other.eq)) {
            return false;
        }
        return true;
    }

}
