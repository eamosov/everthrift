package org.everthrift.appserver.jgroups;

import org.everthrift.appserver.cluster.Slf4jLogImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgroups.logging.CustomLogFactory;
import org.jgroups.logging.Log;
import org.slf4j.LoggerFactory;

public class JGroupsLogFactory implements CustomLogFactory {

    @NotNull
    @Override
    public Log getLog(@NotNull Class clazz) {
        return new Slf4jLogImpl(LoggerFactory.getLogger(clazz));
    }

    @Nullable
    @Override
    public Log getLog(String category) {
        return null;
    }

}