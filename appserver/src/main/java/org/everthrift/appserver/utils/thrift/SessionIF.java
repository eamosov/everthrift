package org.everthrift.appserver.utils.thrift;

import org.jetbrains.annotations.NotNull;

public interface SessionIF {
    @NotNull
    String getCredentials();
}
