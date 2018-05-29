package org.everthrift.clustering.thrift;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.everthrift.appserver.BeanDefinitionHolder;
import org.everthrift.appserver.controller.ConnectionStateHandler;
import org.everthrift.appserver.controller.ThriftController;
import org.everthrift.appserver.controller.ThriftControllerInfo;
import org.everthrift.appserver.jgroups.RpcJGroups;
import org.everthrift.appserver.transport.asynctcp.RpcAsyncTcp;
import org.everthrift.appserver.transport.http.RpcHttp;
import org.everthrift.appserver.transport.jms.RpcJms;
import org.everthrift.appserver.transport.rabbit.RpcRabbit;
import org.everthrift.appserver.transport.tcp.RpcSyncTcp;
import org.everthrift.appserver.transport.websocket.RpcWebsocket;
import org.everthrift.utils.Pair;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.type.StandardMethodMetadata;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by fluder on 25/05/2018.
 */
public class ThriftControllerDiscovery implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ThriftControllerDiscovery.class);

    public static class ServiceDetails {
        public List<String> annotations;

        public ServiceDetails() {
            
        }

        public ServiceDetails(List<String> annotations) {
            this.annotations = annotations;
        }
    }

    private final JChannel yocluster;

    private final JsonInstanceSerializer<ServiceDetails> serializer = new JsonInstanceSerializer<>(ServiceDetails.class);
    private ServiceDiscovery<ServiceDetails> serviceDiscovery;

    private Map<Pair<String, String>, ThriftControllerInfo> localServicesPerAnnotation = new HashMap<>();
    private Map<String, ThriftControllerInfo> localServices = new HashMap<>();

    private Multimap<String, Class<? extends ConnectionStateHandler>> stateHandlers = LinkedListMultimap.create();

    public final Class<? extends Annotation> rpcAnnotations[] = new Class[]{RpcWebsocket.class, RpcHttp.class, RpcJGroups.class, RpcRabbit.class, RpcSyncTcp.class, RpcAsyncTcp.class, RpcJms.class};


    public final String BASE_PATH = "/services";

    private boolean running = false;

    private final BeanDefinitionHolder beanDefinitionHolder;
    private final ThriftServicesDiscovery thriftServicesDiscovery;

    public ThriftControllerDiscovery(BeanDefinitionHolder beanDefinitionHolder,
                                     ThriftServicesDiscovery thriftServicesDiscovery,
                                     CuratorFramework client,
                                     JChannel yocluster) {

        this.beanDefinitionHolder = beanDefinitionHolder;
        this.yocluster = yocluster;
        this.thriftServicesDiscovery = thriftServicesDiscovery;

        serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceDetails.class)
                                                  .client(client)
                                                  .basePath(BASE_PATH)
                                                  .serializer(serializer)
                                                  .build();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        if (callback != null) {
            callback.run();
        }
    }

    private Class getArgsCls(@NotNull Class cls) {
        for (Method m : cls.getMethods()) {
            if (m.getName().equals("setup") && m.getParameterTypes().length == 1 && !m.isBridge()) {
                return m.getParameterTypes()[0];
            }
        }
        return null;
    }

    private List<String> checkAnnotations(@NotNull Class cls, Class<? extends Annotation>... annotations) {
        final List<String> ret = new ArrayList<>();
        for (Class<? extends Annotation> a : annotations) {
            if (cls.getAnnotation(a) != null) {
                ret.add(a.getSimpleName());
            }
        }
        return ret;
    }

    public List<ThriftControllerInfo> getLocal(String annName) {
        return localServicesPerAnnotation.entrySet()
                                         .stream()
                                         .filter(e -> e.getKey().first.equals(annName))
                                         .map(Map.Entry::getValue)
                                         .collect(Collectors.toList());
    }

    public ThriftControllerInfo getLocal(String annName, String fullMethodName) {
        return localServicesPerAnnotation.get(new Pair<>(annName, fullMethodName));
    }

    public List<Address> getCluster(String annName, String fullMethodName) {
        try {
            final List<Address> addresses =  serviceDiscovery.queryForInstances(fullMethodName)
                                   .stream()
                                   .filter(i -> i.getPayload().annotations.contains(annName))
                                   .map(i -> UUID.fromString(i.getAddress()))
                                   .collect(Collectors.toList());

            Collections.shuffle(addresses);
            return addresses;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }


    public Collection<Class<? extends ConnectionStateHandler>> getConnectionStateHandlers(String annName) {
        return stateHandlers.get(annName);
    }

    private final static Pattern serviceArgs = Pattern.compile("(.*)\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_-]+)_args");

    public static Pair<String, String> extractMethodName(@NotNull Class argsCls) {
        final Matcher m = serviceArgs.matcher(argsCls.getCanonicalName());
        if (m.matches()) {
            return Pair.create(m.group(2), m.group(3));
        } else {
            return null;
        }
    }

    @NotNull
    public ThriftControllerInfo registerController(@NotNull List<String> annotations, @NotNull String beanName, @NotNull Class beanCls, @NotNull String fullMethodName) {

        return localServices.computeIfAbsent(fullMethodName, _f -> {
            try {
                final ServiceInstance<ServiceDetails> serviceInstance =
                    ServiceInstance.<ServiceDetails>builder().name(fullMethodName)
                                                             .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                                                             .address(((org.jgroups.util.UUID) yocluster.getAddress()).toStringLong()) // Service information
                                                             .payload(new ServiceDetails(annotations))
                                                             .port(0) // Port and payload
                                                             .build(); // this instance definition

                serviceDiscovery.registerService(serviceInstance);

                final ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry = thriftServicesDiscovery.getByMethod(serviceInstance
                                                                                                                            .getName());

                if (thriftMethodEntry == null) {
                    throw new RuntimeException("couldn't find ThriftMethodEntry for " + serviceInstance.getName());
                }

                final ThriftControllerInfo controllerInfo = new ThriftControllerInfo(beanName, beanCls, thriftMethodEntry);

                for (String ann : serviceInstance.getPayload().annotations) {
                    localServicesPerAnnotation.put(new Pair<>(ann, fullMethodName), controllerInfo);
                }

                log.info("Registered service: {}", fullMethodName);

                return controllerInfo;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public void start() {
        running = true;

        for (String beanName : ImmutableSet.copyOf(beanDefinitionHolder.getBeanDefinitionRegistry()
                                                                       .getBeanDefinitionNames())) {
            final BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinitionRegistry()
                                                                      .getBeanDefinition(beanName);

            if (beanDefinition.isPrototype()) {

                Class beanCls = null;
                if (beanDefinition.getBeanClassName() != null) {
                    try {
                        beanCls = Class.forName(beanDefinition.getBeanClassName());
                    } catch (ClassNotFoundException e) {
                    }
                } else if (beanDefinition.getSource() instanceof StandardMethodMetadata) {
                    beanCls = ((StandardMethodMetadata) beanDefinition.getSource()).getIntrospectedMethod()
                                                                                   .getReturnType();
                }

                if (beanCls != null) {
                    if (ThriftController.class.isAssignableFrom(beanCls)) {

                        final Pair<String, String> methodName = extractMethodName(getArgsCls(beanCls));

                        if (methodName != null) {
                            registerController(checkAnnotations(beanCls, rpcAnnotations), beanName, beanCls, methodName.first + ":" + methodName.second);
                        }

                    } else if (ConnectionStateHandler.class.isAssignableFrom(beanCls)) {
                        for (String a : checkAnnotations(beanCls, rpcAnnotations)) {
                            stateHandlers.put(a, beanCls);
                        }
                    }
                }
            }
        }

        try {
            serviceDiscovery.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {

        try {
            serviceDiscovery.close();
        } catch (Exception e) {
        }

        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 5;
    }
}
