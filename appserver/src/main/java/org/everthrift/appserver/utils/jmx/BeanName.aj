package org.everthrift.appserver.utils.jmx;

public aspect BeanName {

    private String BeanNameHolder.beanName = null;

    public String BeanNameHolder.getBeanName() {
        return beanName;
    }

    public void BeanNameHolder.setBeanName(String value) {
        beanName = value;
    }
}
