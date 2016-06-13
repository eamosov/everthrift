/*
 * Copyright 2012-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.knockchat.appserver;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.knockchat.appserver.configs.AppserverConfig;
import com.knockchat.appserver.configs.AsyncTcpThrift;
import com.knockchat.appserver.configs.JGroups;
import com.knockchat.appserver.configs.LoopbackJGroups;
import com.knockchat.appserver.configs.TcpThrift;
import com.knockchat.cassandra.migrator.CMigrationProcessor;
import com.knockchat.sql.migration.MigrationProcessor;
import com.knockchat.utils.SocketUtils;
import com.knockchat.utils.thrift.MetaDataMapBuilder;

public class AppserverApplication {


    private static final Logger log = LoggerFactory.getLogger(AppserverApplication.class);
    public final static AppserverApplication INSTANCE = new AppserverApplication();

    public final AnnotationConfigApplicationContext context;
    public final ConfigurableEnvironment env;
    //	private ClassPathScanningCandidateComponentProvider scanner;
    private final List<String> scanPathList = new ArrayList<String>();
    @SuppressWarnings("rawtypes")
    private final List<PropertySource> propertySourceList = new ArrayList<PropertySource>();

    private boolean initialized = false;

    
    private List<Class> annotatedClasses = Lists.newArrayList(AppserverConfig.class);

    private AppserverApplication() {

        System.setProperty("jgroups.logging.log_factory_class", "com.knockchat.appserver.cluster.JGroupsLogFactory");

        context = new AnnotationConfigApplicationContext();
        env = context.getEnvironment();

        scanPathList.add("com.knockchat.appserver");
    }
    

    private boolean isJettyEnabled(){
    	return !env.getProperty("jetty", "false").equalsIgnoreCase("false");
    }

    private boolean isThriftEnabled(){
    	return !env.getProperty("thrift", "false").equalsIgnoreCase("false");
    }

    private boolean isAsyncThriftEnabled(){
    	return !env.getProperty("thrift.async", "false").equalsIgnoreCase("false");
    }

    public static boolean isJGroupsEnabled(Environment env){
    	return !env.getProperty("jgroups", "false").equalsIgnoreCase("false");
    }

    private boolean isJGroupsEnabled(){
    	return isJGroupsEnabled(env);
    }

    private boolean isJmsEnabled(){
    	return !env.getProperty("jms", "false").equalsIgnoreCase("false");
    }
    
    private boolean isRabbitEnabled(){
    	return !env.getProperty("rabbit", "false").equalsIgnoreCase("false");
    }

    @SuppressWarnings("rawtypes")
    public synchronized void init(String[] args, String version) {

        final Resource resource = context.getResource("classpath:application.properties");

        try {
            final ResourcePropertySource ps = new ResourcePropertySource(resource);
            env.getPropertySources().addFirst(ps);
        } catch (IOException e1) {
            log.error("Coudn't load application.properties", e1);
        }

        for (PropertySource p : propertySourceList)
            env.getPropertySources().addFirst(p);

        env.getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));
        env.getPropertySources().addLast(new MapPropertySource("thriftScanPathList", Collections.singletonMap("thrift.scan", (Object) scanPathList)));
        env.getPropertySources().addLast(new MapPropertySource("version", Collections.singletonMap("version", (Object) version)));

        if (Boolean.parseBoolean(env.getProperty("sqlmigrator.run", "false"))) {
            runSqlMigrator();
        }
        
        if (Boolean.parseBoolean(env.getProperty("cassandra.migrator.run", "false"))) {
        	runCMigrationProcessor();
        }
                
        final boolean nothrift = !env.getProperty("nothrift", "false").equalsIgnoreCase("false");

        try {
            if (env.getProperty("thrift.async.port").equals("0"))
                addProperty("thrift.async.port", SocketUtils.findAvailableServerSocket());

            if (!nothrift && env.getProperty("thrift.port").equals("0"))
                addProperties("thrift.port", SocketUtils.findAvailableServerSocket());

        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
        
        if (env.getProperty("jgroups.multicast.bind_addr") !=null)
        	System.setProperty("jgroups.multicast.bind_addr", env.getProperty("jgroups.multicast.bind_addr"));

        if (env.getProperty("tbase.root") !=null){
    		final MetaDataMapBuilder mdb = new MetaDataMapBuilder();

        	for (String root : env.getProperty("tbase.root").split(",")){
        		mdb.build(root);		
        	}
        }

        context.register(annotatedClasses.toArray(new Class[annotatedClasses.size()]));
        
        if (isAsyncThriftEnabled())
        	context.register(AsyncTcpThrift.class);
        
        if (isThriftEnabled())
        	context.register(TcpThrift.class);

        if (isJGroupsEnabled())
        	context.register(JGroups.class);
        else        
        	context.register(LoopbackJGroups.class);
        
        if (isJmsEnabled()){
        	try {
        		context.register(Class.forName("com.knockchat.jms.Jms"));
			} catch (ClassNotFoundException e) {
				throw Throwables.propagate(e);
			}        		
        }else{
        	try {        	
        		context.register(Class.forName("com.knockchat.jms.LoopbackJms"));
			} catch (ClassNotFoundException e) {
				log.warn("Cound't find LoopbackJms. @RpcJms Service will be disabled.");
			}        		        	
        }
        
        if (isRabbitEnabled()){
        	try {
        		context.register(Class.forName("com.knockchat.rabbit.RabbitConfig"));
			} catch (ClassNotFoundException e) {
				throw Throwables.propagate(e);
			}        		        	
        }
        
        if (isJettyEnabled()) {
        	try {
				context.register(Class.forName("com.knockchat.jetty.configs.Http"));
			} catch (ClassNotFoundException e) {
				throw Throwables.propagate(e);
			}
        }
        
        try {
			context.register(Class.forName("com.knockchat.cassandra.model.CassandraConfig"));
		} catch (ClassNotFoundException e) {
		}
        
        context.refresh();
        initialized = true;        
    }

    private void runSqlMigrator() {
        final ClassPathXmlApplicationContext xmlContext = new ClassPathXmlApplicationContext(new String[]{"migration-context.xml"}, false);
        try {
            for (final PropertySource p : env.getPropertySources())
                xmlContext.getEnvironment().getPropertySources().addLast(p);
            
            xmlContext.refresh();

            final MigrationProcessor processor = xmlContext.getBean(MigrationProcessor.class);
            if (processor == null) {
                log.info("Can't find Migration processor bean");
                return;
            }
            final Map<String, MigrationProcessor.Result> results = processor.process(true, Collections.EMPTY_LIST, false);
            for (final Map.Entry<String, MigrationProcessor.Result> entry : results.entrySet())
                if (entry.getValue().equals(MigrationProcessor.Result.FAIL)) {
                    log.error("Filed to execute migration: {} ", entry.getKey());
                    throw Throwables.propagate(entry.getValue().getException());
                }
        } finally {
            if (xmlContext != null && xmlContext.isActive())
                xmlContext.close();
        }
    }
    
    private void runCMigrationProcessor() {
    	
    	log.info("Executing cassandra migrations");
    	
        final ClassPathXmlApplicationContext xmlContext = new ClassPathXmlApplicationContext(new String[]{"classpath:cassandra-migration-context.xml"}, false);
        try {
            for (final PropertySource p : env.getPropertySources())
                xmlContext.getEnvironment().getPropertySources().addLast(p);
            
            xmlContext.refresh();

            final CMigrationProcessor processor = xmlContext.getBean(CMigrationProcessor.class);
            if (processor == null) {
                log.info("Can't find CMigrationProcessor bean");
                return;
            }
            
            processor.migrate();
            
        } finally {
            if (xmlContext != null && xmlContext.isActive())
                xmlContext.close();
        }
    }
    

    private KeyStore loadJettyKeystore(final String jks, String keystorePassword) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
        final InputStream inputStream = context.getResource(jks).getInputStream();
        final KeyStore keyStore;

        try {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(inputStream, keystorePassword != null ? keystorePassword.toCharArray() : null);
        } finally {
            if (inputStream != null)
                inputStream.close();
        }

        return keyStore;
    }

    public synchronized void start() {
    }

    public synchronized void stop() {

        context.close();
    }

    /**
     * Add path to thrift controllers scan path
     * @param p
     */
    public synchronized void addScanPath(String p) {
        scanPathList.add(p);
    }

    public synchronized List<String> getScanPathList() {
        return scanPathList;
    }

    @SuppressWarnings("rawtypes")
    public synchronized void addPropertySource(PropertySource ps) {
        if (!initialized)
            propertySourceList.add(ps);
        else
            env.getPropertySources().addFirst(ps);
    }

    public synchronized void addPropertySource(String resourceName) throws IOException {
        addPropertySource(new ResourcePropertySource(AppserverApplication.INSTANCE.context.getResource(resourceName)));
    }
    
    public void registerAnnotatedClasses(Class<?>... annotatedClasses) {
    	for (Class cls: annotatedClasses)
    		this.annotatedClasses.add(cls);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public synchronized void addProperties(String name, Object... key_value) throws IOException {

        if (key_value.length % 2 != 0) {
            throw new IllegalArgumentException();
        }

        final Map props2 = new HashMap();

        for (int i = 0; i <= key_value.length - 2; i = i + 2) {

            if (!(key_value[i] instanceof String))
                throw new IllegalArgumentException(String.format("i=%d, type=%s", i, key_value[i].getClass().toString()));

            props2.put(key_value[i], key_value[i + 1]);
        }

        addPropertySource(new MapPropertySource(name, props2));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public synchronized void addProperty(String name, Object value) throws IOException {
        final Map props2 = new HashMap();
        props2.put(name, value);
        addPropertySource(new MapPropertySource(name, props2));
    }

    public void waitExit() {
        while (true) {

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }

        System.out.println("Exiting application...bye.");
        System.exit(0);
    }
}
