package org.everthrift.appserver.utils.jmx;

import org.jetbrains.annotations.NotNull;

public interface BeanNameHolder {
    void setBeanName(String name);

    @NotNull
    String getBeanName();
}
