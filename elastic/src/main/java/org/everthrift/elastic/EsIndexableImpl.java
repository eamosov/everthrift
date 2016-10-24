package org.everthrift.elastic;

import org.apache.commons.lang.NotImplementedException;

import java.io.Serializable;

/**
 * Created by fluder on 22.10.16.
 */
public class EsIndexableImpl implements EsIndexableIF{

    private final Serializable pk;
    private final long version;

    public EsIndexableImpl(Serializable pk, long version) {
        this.version = version;
        this.pk = pk;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public Serializable getPk() {
        return pk;
    }

    @Override
    public void setPk(Serializable identifier) {
        throw new NotImplementedException();
    }
}
