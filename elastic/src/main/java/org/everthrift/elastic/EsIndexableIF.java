package org.everthrift.elastic;

import org.everthrift.appserver.model.DaoEntityIF;

public interface EsIndexableIF extends DaoEntityIF{
    long getVersion();
}
