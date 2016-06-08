package com.knockchat.sql.jmx;

import java.util.Map;

import javax.management.modelmbean.ModelMBean;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.MBeanExportException;
import org.springframework.jmx.export.annotation.AnnotationMBeanExporter;

public class PersistanceMbeanExporter extends AnnotationMBeanExporter {

	@Autowired
	private ApplicationPropertiesModelFactory propertiesModelFactory;

    public PersistanceMbeanExporter() {
        super();
    }

    private ListableBeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory bf) {
        if (bf instanceof ListableBeanFactory) {
            this.beanFactory = (ListableBeanFactory) bf;
        }
        super.setBeanFactory(bf);
    }

    @Override
	protected ModelMBean createAndConfigureMBean(Object managedResource, String beanKey) throws MBeanExportException {
        return (ModelMBean) getProxy(super.createAndConfigureMBean(managedResource, beanKey), managedResource);
    }

    private Object getProxy(Object wrapper, Object bean) {
        final String name = getBeanName(bean);
        if (name == null) {
            return wrapper;
        }
        ProxyFactory factory = new ProxyFactory(wrapper);
        factory.addAdvice(new PersistMBeanInterceptor(bean, propertiesModelFactory));
        return factory.getProxy();
    }

    private String getBeanName(Object target) {
        if (beanFactory!=null) {
            for (Object o : beanFactory.getBeansOfType(target.getClass()).entrySet()) {
                Map.Entry entry = (Map.Entry) (o);
                if (entry.getValue() == target) {
                    return (String) entry.getKey();
                }
            }
        }
        return null;
    }

}
