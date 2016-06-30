package org.everthrift.appserver.controller;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

public abstract class ThriftControllerRegistry implements InitializingBean{

    public final Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected ApplicationContext applicationContext;

    private Map<String, ThriftControllerInfo> map = Collections.synchronizedMap(new HashMap<String, ThriftControllerInfo>());
    private List<Class<ConnectionStateHandler>> stateHandlers =  new CopyOnWriteArrayList<Class<ConnectionStateHandler>>();

    private final Class<? extends Annotation> annotationType;

    public ThriftControllerRegistry(Class<? extends Annotation> annotationType){
        //scanThriftControllers(annotationType);
        this.annotationType = annotationType;
    }

    public Class<? extends Annotation> getType(){
        return annotationType;
    }

    private void scanThriftControllers(Class<? extends Annotation> annotationType){

        final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);

        scanner.addIncludeFilter(new AnnotationTypeFilter(annotationType));

        final List<String> l = applicationContext.getEnvironment().getProperty("thrift.scan", List.class);

        for (String p:l)
            scanThriftControllers(scanner, p);
    }


    private void scanThriftControllers(ClassPathScanningCandidateComponentProvider scanner, String basePath){
        for (BeanDefinition b : scanner.findCandidateComponents(basePath)){
            final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());

            if (ConnectionStateHandler.class.isAssignableFrom(cls)){

                stateHandlers.add((Class<ConnectionStateHandler>)cls);

            }else if (ThriftController.class.isAssignableFrom(cls)){
                for (Method m: cls.getMethods()){
                    if (m.getName().equals("setup")){
                        try{
                            final ThriftControllerInfo i = tryRegisterController(cls, m.getParameterTypes()[0]);
                            log.debug("Find ThriftController: {}", i);
                            map.put(i.getName(), i);
                            break;
                        }catch(IllegalArgumentException e){
                        }
                    }
                }
            }
        }
    }

    public ThriftControllerInfo getController(String name){
        return map.get(name);
    }

    public Set<String> getContollerNames(){
        return map.keySet();
    }

    public Map<String, ThriftControllerInfo> getControllers(){
        return map;
    }

    private ThriftControllerInfo tryRegisterController(final Class cls, Class argument){

        if (!TBase.class.isAssignableFrom(argument)){
            log.debug("Result class {} is not TBase", argument.getSimpleName());
            throw new IllegalArgumentException ();
        }

        final Pattern serviceArgs = Pattern.compile("(.*)\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_-]+)_args");
        final Matcher m = serviceArgs.matcher(argument.getCanonicalName());

        if (!m.matches()){
            log.trace("Argument '{}' not matches pattern", argument.getCanonicalName());
            throw new IllegalArgumentException ();
        }

        final String service= m.group(2);
        final String method = m.group(3);
        final Class<?> resultWrap = ClassUtils.resolveClassName(m.group(1) + "." + m.group(2) + "." + m.group(3) + "_result", ClassUtils.getDefaultClassLoader());

        Method findResultFieldByName = null;

        for (Class i: resultWrap.getDeclaredClasses()){
            if (i.getCanonicalName().endsWith("_Fields")){

                try {
                    findResultFieldByName = i.getMethod("findByName", String.class);
                } catch (SecurityException e) {
                    throw new IllegalArgumentException ();
                } catch (NoSuchMethodException e) {
                    throw new IllegalArgumentException ();
                }

                break;
            }
        }

        if (findResultFieldByName == null){
            log.error("Coudn't find {}._Fields", resultWrap.getCanonicalName());
            throw new IllegalArgumentException ();
        }

        final ThriftControllerInfo i = new ThriftControllerInfo(applicationContext, (Class<? extends ThriftController>)cls, service, method, (Class<? extends TBase>)argument, (Class<? extends TBase>)resultWrap, findResultFieldByName);
        return i;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        scanThriftControllers(annotationType);
    }

    public List<Class<ConnectionStateHandler>> getStateHandlers() {
        return stateHandlers;
    }

}
