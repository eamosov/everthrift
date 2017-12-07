package org.everthrift.appserver;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

/**
 * Created by fluder on 24/10/2017.
 */
public class BeanDefinitionHolder implements BeanFactoryPostProcessor {

    private ConfigurableListableBeanFactory factory;

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory factory) throws BeansException {
        this.factory = factory;
    }

    @NotNull
    public BeanDefinitionRegistry getBeanDefinitionRegistry() {
        return ((BeanDefinitionRegistry) factory);
    }
}
