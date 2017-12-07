package org.everthrift.appserver.scheduling.context;


import org.everthrift.appserver.scheduling.ContextAccessError;
import org.jetbrains.annotations.Nullable;

import java.util.Date;

public interface TriggerContextAccessor {

    @Nullable
    SettableTriggerContext get() throws ContextAccessError;

    boolean update(SettableTriggerContext ctx) throws ContextAccessError;

    void updateLastCompletionTime(Date time) throws ContextAccessError;
}