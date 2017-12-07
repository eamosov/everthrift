package org.everthrift.appserver.utils.jmx;

import org.jetbrains.annotations.Nullable;

public aspect BeanName {

    @Nullable private String BeanNameHolder.beanName = null;

    @Nullable public String BeanNameHolder.getBeanName() {
        return beanName;
    }

    public void BeanNameHolder.setBeanName(String value) {
        beanName = value;
    }
}
