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
import java.lang.management.ManagementFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jminix.console.servlet.MiniConsoleServlet;
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
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsProcessor;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;

import com.google.common.base.Throwables;
import com.knockchat.appserver.configs.AsyncTcpThrift;
import com.knockchat.appserver.configs.Config;
import com.knockchat.appserver.configs.Http;
import com.knockchat.appserver.configs.JGroups;
import com.knockchat.appserver.configs.Jms;
import com.knockchat.appserver.configs.LoopbackJGroups;
import com.knockchat.appserver.configs.LoopbackJms;
import com.knockchat.appserver.configs.TcpThrift;
import com.knockchat.appserver.monitoring.RpsServlet;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.transport.http.BinaryThriftServlet;
import com.knockchat.appserver.transport.http.JsonThriftServlet;
import com.knockchat.cassandra.migrator.CMigrationProcessor;
import com.knockchat.sql.migration.MigrationProcessor;
import com.knockchat.utils.PosAppInitializingBean;
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
    private Server jettyServer;
    public NodeAddress jettyAddress;

    private boolean initialized = false;

    private String webContextConfigLocation;

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

        if (env.getProperty("migrator.run", "false").equalsIgnoreCase("true")) {
            log.info("Try find migrations in {}", env.getProperty("migrator.root.package"));
            runMigrator(env.getProperty("migrator.root.package"));
        }
        
        if (env.getProperty("cassandra.migrator.run", "false").equalsIgnoreCase("true")) {
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

        context.register(Config.class);
        
        if (isAsyncThriftEnabled())
        	context.register(AsyncTcpThrift.class);
        
        if (isThriftEnabled())
        	context.register(TcpThrift.class);

        if (isJGroupsEnabled())
        	context.register(JGroups.class);
        else        
        	context.register(LoopbackJGroups.class);
        
        if (isJmsEnabled())
        	context.register(Jms.class);
        else
        	context.register(LoopbackJms.class);
        
        if (isJettyEnabled()) {
        	context.register(Http.class);
        }
        
        context.refresh();

        if (webContextConfigLocation == null)
            try {
                webContextConfigLocation = context.getResource("/web-context.xml").getURL().toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        initialized = true;

        if (isJettyEnabled()) {
            initJetty();
        }
        
        for (PosAppInitializingBean bean :context.getBeansOfType(PosAppInitializingBean.class).values())
			try {
				bean.afterAppInitizlized();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
    }

    private void runMigrator(String rootPackage) {
        final ClassPathXmlApplicationContext xmlContext = new ClassPathXmlApplicationContext(new String[]{"migration-context.xml"}, false);
        try {
            for (final PropertySource p : env.getPropertySources())
                xmlContext.getEnvironment().getPropertySources().addLast(p);
            xmlContext.getEnvironment().getPropertySources().addFirst(new MapPropertySource("root.packages", Collections.singletonMap("root.packages", (Object) Arrays.asList(rootPackage))));
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

    private void initJetty() {

        final String host = env.getProperty("jetty.host");
        final int port = Integer.parseInt(env.getProperty("jetty.port"));
        final int sslPort = Integer.parseInt(env.getProperty("jetty.ssl.port", "443"));

        log.info("Starting jetty server on {}:{}", host, port);

        final int capacity = Integer.parseInt(env.getProperty("jetty.capacity", "6000"));
        final int maxThreads = Integer.parseInt(env.getProperty("jetty.maxThreads", "10"));
        final int minThreads = Integer.parseInt(env.getProperty("jetty.minThreads", "10"));
        final int idleTimeout = Integer.parseInt(env.getProperty("jetty.idleTimeout", "60000"));

        final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(capacity);

        final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout, queue);
        threadPool.setName("jetty");

        jettyServer = new Server(threadPool);
        
        final MBeanContainer mbContainer=new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
        jettyServer.addEventListener(mbContainer);
        jettyServer.addBean(mbContainer);
        
        // Register loggers as MBeans
        jettyServer.addBean(Log.getLog());

        final HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        http_config.setSecurePort(sslPort);
        http_config.setOutputBufferSize(32768);
        http_config.setRequestHeaderSize(8192);
        http_config.setResponseHeaderSize(8192);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);

        final ServerConnector http = new ServerConnector(jettyServer, new HttpConnectionFactory(http_config));
        http.setHost(host);
        http.setPort(port);
        jettyServer.addConnector(http);

        final String pkcs12 = env.getProperty("jetty.ssl.jks.path");
        if (pkcs12 != null) {

            try {
                final KeyStore keyStore = loadJettyKeystore(pkcs12, env.getProperty("jetty.ssl.jks.pass"));

                final SslContextFactory contextFactory = new SslContextFactory();

                contextFactory.setKeyStorePassword(env.getProperty("jetty.ssl.jks.pass"));
                contextFactory.setTrustStorePassword(env.getProperty("jetty.ssl.jks.pass"));
                contextFactory.setKeyStore(keyStore);
                contextFactory.setTrustStore(keyStore);

                final HttpConfiguration https_config = new HttpConfiguration(http_config);
                https_config.addCustomizer(new SecureRequestCustomizer());

                final ServerConnector https = new ServerConnector(jettyServer, new SslConnectionFactory(contextFactory, HttpVersion.HTTP_1_1.toString()), new HttpConnectionFactory(https_config));
                https.setHost(host);
                https.setPort(sslPort);
                jettyServer.addConnector(https);
            } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {

                throw new RuntimeException(e);
            }
        }

        jettyAddress = new NodeAddress(host, port);

        //final ServletContextHandler jettyContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
        final WebAppContext jettyContext = new WebAppContext();
        jettyContext.setContextPath("/");
        jettyContext.setResourceBase("webapp");
        try {
            jettyContext.setDescriptor(context.getResource("/WEB-INF/web.xml").getURL().toString());
        } catch (IOException e) {
            log.error("Coudn't load web.xml", e);
        }
        jettyContext.setConfigurationClasses(WebAppContext.getDefaultConfigurationClasses());
        jettyServer.setHandler(jettyContext);

        if (env.getProperty("jminix") != null) {
            final MiniConsoleServlet jminix = new MiniConsoleServlet();
            jettyContext.addServlet(new ServletHolder(jminix), "/jmx/*");
        }
        
        final JsonThriftServlet jsonThriftServlet = context.getBean(JsonThriftServlet.class);
        jettyContext.addServlet(new ServletHolder(jsonThriftServlet), "/TJSON");

        final BinaryThriftServlet binaryThriftServlet = context.getBean(BinaryThriftServlet.class);
        jettyContext.addServlet(new ServletHolder(binaryThriftServlet), "/TBINARY");
                
        final RpsServlet rpsServlet = context.getBean(RpsServlet.class);
        jettyContext.addServlet(new ServletHolder(rpsServlet), "/rps");

        //final AnnotationConfigWebApplicationContext springWebApplicationContext = new AnnotationConfigWebApplicationContext();
        final XmlWebApplicationContext springWebApplicationContext = new XmlWebApplicationContext();
        springWebApplicationContext.setParent(context);
        springWebApplicationContext.setConfigLocation(webContextConfigLocation);
        //springWebApplicationContext.register(ServletConfig.class);

        final DispatcherServlet dispatcherServlet = new DispatcherServlet(springWebApplicationContext);
        jettyContext.addServlet(new ServletHolder(dispatcherServlet), "/*");
        
        springWebApplicationContext.refresh();
        
        //hack for disable CORS
        for (AbstractHandlerMapping m: springWebApplicationContext.getBeansOfType(AbstractHandlerMapping.class).values()){
        	m.setCorsProcessor(new CorsProcessor(){

				@Override
				public boolean processRequest(CorsConfiguration configuration, HttpServletRequest request, HttpServletResponse response) throws IOException {					
					return true;
				}});
        }
			
    }

    public synchronized void start() {
        try {
            if (jettyServer != null)
                jettyServer.start();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void stop() {

        try {
            if (jettyServer != null)
                jettyServer.stop();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        context.close();
    }

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

    public String getWebContextConfigLocation() {
        return webContextConfigLocation;
    }

    public void setWebContextConfigLocation(String webContextConfigLocation) {
        this.webContextConfigLocation = webContextConfigLocation;
    }

//	public static void main(String[] args) {
//		INSTANCE.init(args, AppserverApplication.class.getPackage().getImplementationVersion());
//		INSTANCE.start();
//		INSTANCE.waitExit();
//	}
}
