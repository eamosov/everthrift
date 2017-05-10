package org.everthrift.appserver.utils.zooprops;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonWriter;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.thrift.TBase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.everthrift.appserver.utils.thrift.GsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.context.ApplicationContext;

import javax.management.Attribute;
import javax.management.AttributeList;
import java.beans.PropertyDescriptor;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

class ZPersistMBeanInterceptor implements MethodInterceptor {

    private static final Logger log = LoggerFactory.getLogger(ZPersistMBeanInterceptor.class);

    private final NodeCache nodeCache;

    private final Object bean;

    private final CuratorFramework curator;

    private final String zooPath;

    private static final String SET_ATTRIBUTE = "setAttribute";

    private static final String SET_ATTRIBUTES = "setAttributes";

    private static final Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(TBase.class, new GsonSerializer.TBaseSerializer(FieldNamingPolicy.IDENTITY, true))
                                                      .setPrettyPrinting()
                                                      .create();


    ZPersistMBeanInterceptor(Object bean, final String rootPath, final String persistName, CuratorFramework curator, ApplicationContext context) throws Exception {
        this.bean = bean;
        this.zooPath = rootPath + "/beans/" + persistName;
        this.curator = curator;

        Stat s = curator.checkExists().forPath(zooPath);
        if (s == null) {
            log.warn("Creating path {}", zooPath);

            final String profile = context.getEnvironment().getProperty("spring.profiles.active");

            if (profile == null) {
                throw new RuntimeException("Coudn't load defaults, profile is null");
            }

            curator.create()
                   .creatingParentContainersIfNeeded()
                   .forPath(zooPath, IOUtils.toByteArray(context.getResource("classpath:zooprops/" + profile + "/" + persistName + ".json")
                                                                .getInputStream()));
        }

        this.nodeCache = new NodeCache(curator, zooPath);
        this.nodeCache.start(true);

        if (nodeCache.getCurrentData() == null) {
            throw new RuntimeException("Error getting data for path " + zooPath);
        }

        this.nodeCache.getListenable().addListener(() -> load());

        this.load();
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        String method = invocation.getMethod().getName();
        Object[] args = invocation.getArguments();

        if (method.equals(SET_ATTRIBUTE) && args.length == 1 && args[0] instanceof Attribute) {

            Attribute attribute = (Attribute) (args[0]);
            storeProperty(attribute.getName(), attribute.getValue());

        } else if (method.equals(SET_ATTRIBUTES) && args.length == 1 && args[0] instanceof AttributeList) {

            for (Object object : (AttributeList) (args[0])) {
                Attribute attribute = (Attribute) object;
                storeProperty(attribute.getName(), attribute.getValue());
            }
        }
        return invocation.proceed();
    }

    private void load() {

        final ChildData cd = nodeCache.getCurrentData();

        if (cd.getData() == null) {
            return;
        }

        final JsonParser parser = new JsonParser();
        final JsonElement js = parser.parse(new String(cd.getData(), Charsets.UTF_8));
        if (js == null) {
            return;
        }

        final Object parsed = gson.fromJson(js, bean.getClass());

        if (parsed == null) {
            return;
        }

        final PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(bean.getClass());

        for (PropertyDescriptor pd : pds) {
            if (js.getAsJsonObject().has(pd.getName()) && pd.getReadMethod() != null && pd.getWriteMethod() != null) {
                try {
                    final Object v = pd.getReadMethod().invoke(parsed);
                    pd.getWriteMethod().invoke(bean, v);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    void storeProperty(String attributeName) throws Exception {

        final PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(bean.getClass());
        String name = null;
        Object value = null;

        for (PropertyDescriptor pd : pds) {
            if (pd.getName().equalsIgnoreCase(attributeName)) {
                name = pd.getName();
                value = pd.getReadMethod().invoke(bean);
            }
        }

        if (name == null) {
            throw new Exception("Coudn't find property " + attributeName + " in class " + bean.getClass()
                                                                                              .getSimpleName());
        }

        store(name, value);
    }

    private void storeProperty(String attributeName, Object value) throws Exception {

        String name = null;

        final PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(bean.getClass());
        for (PropertyDescriptor pd : pds) {
            if (pd.getName().equalsIgnoreCase(attributeName)) {
                name = pd.getName();
            }
        }

        if (name == null) {
            log.error("Coudn't find attribute {} in class {}", attributeName, bean.getClass().getSimpleName());
            return;
        }

        store(name, value);
    }

    private void store(String name, Object value) throws Exception {

        boolean badVersion;

        do {
            badVersion = false;
            final JsonParser parser = new JsonParser();
            final ChildData cd = nodeCache.getCurrentData();
            final JsonObject jo;

            if (cd.getData() != null) {
                jo = parser.parse(new String(cd.getData(), Charsets.UTF_8)).getAsJsonObject();
            } else {
                jo = new JsonObject();
            }

            jo.add(name, value != null ? gson.toJsonTree(value) : null);

            final StringWriter stringWriter = new StringWriter();
            final JsonWriter jsonWriter = new JsonWriter(stringWriter);
            jsonWriter.setLenient(true);
            jsonWriter.setIndent("  ");
            Streams.write(jo, jsonWriter);

            try {
                curator.setData()
                       .withVersion(cd.getStat().getVersion())
                       .forPath(zooPath, stringWriter.toString().getBytes(Charsets.UTF_8));
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.BADVERSION) {
                    badVersion = true;
                } else {
                    throw e;
                }
            }
        } while (badVersion);
    }
}
