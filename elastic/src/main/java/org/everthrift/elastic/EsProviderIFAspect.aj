package org.everthrift.elastic;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

/**
 * Created by fluder on 21.10.16.
 */
public aspect EsProviderIFAspect {
    private ApplicationContext EsProviderIF.applicationContext;

    public void EsProviderIF.setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public ApplicationContext EsProviderIF.getApplicationContext() {
        return this.applicationContext;
    }
}
