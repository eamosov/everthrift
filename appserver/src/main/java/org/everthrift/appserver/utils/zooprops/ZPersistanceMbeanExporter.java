package org.everthrift.appserver.utils.zooprops;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.jetbrains.annotations.NotNull;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.jmx.export.MBeanExportException;
import org.springframework.jmx.export.annotation.AnnotationMBeanExporter;
import org.springframework.jmx.export.annotation.ManagedResource;

import javax.management.modelmbean.ModelMBean;
import java.util.Map;

public class ZPersistanceMbeanExporter extends AnnotationMBeanExporter {

    private final CuratorFramework curator;
    private final ApplicationContext context;
    private final String rootPath;

    private final Map<Object, ZPersistMBeanInterceptor>  interceptors = Maps.newIdentityHashMap();

    public ZPersistanceMbeanExporter(CuratorFramework curator, ApplicationContext context, @Value("${zookeeper.rootPath}") String rootPath) {
        super();
        this.curator = curator;
        this.context = context;
        this.rootPath = rootPath;
    }

    private ListableBeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory bf) {
        if (bf instanceof ListableBeanFactory) {
            this.beanFactory = (ListableBeanFactory) bf;
        }
        super.setBeanFactory(bf);
    }

    @NotNull
    @Override
    synchronized protected ModelMBean createAndConfigureMBean(@NotNull Object managedResource, String beanKey) throws MBeanExportException {
        try {
            return getProxy(super.createAndConfigureMBean(managedResource, beanKey), managedResource);
        } catch (Exception e) {
            Throwables.propagateIfPossible(e);
            throw new MBeanExportException("Exception", e);
        }
    }

    @NotNull
    private ModelMBean getProxy(@NotNull ModelMBean modelMbean, @NotNull Object managedResource) throws Exception {
        final String name = getBeanName(managedResource);
        if (name == null) {
            return modelMbean;
        }

        final ManagedResource mr = managedResource.getClass().getDeclaredAnnotation(ManagedResource.class);
        if (mr == null || mr.persistName().isEmpty()) {
            return modelMbean;
        }

        final ProxyFactory factory = new ProxyFactory(modelMbean);
        final ZPersistMBeanInterceptor i = new ZPersistMBeanInterceptor(managedResource, rootPath, mr.persistName(), curator, context);
        factory.addAdvice(i);

        interceptors.put(managedResource, i);
        return (ModelMBean) factory.getProxy();
    }

    private synchronized ZPersistMBeanInterceptor getInterceptor(Object managedResource){
        return interceptors.get(managedResource);
    }

    public void store(@NotNull Object managedResource, String propertyName){
        final ZPersistMBeanInterceptor i = getInterceptor(managedResource);
        if ( i == null)
            throw new RuntimeException("Coudn't find interceptor for bean:" + managedResource.toString());

        try {
            i.storeProperty(propertyName);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private String getBeanName(@NotNull Object target) {
        if (beanFactory != null) {
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
