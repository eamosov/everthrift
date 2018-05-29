package org.everthrift.jetty;

import com.google.common.base.Throwables;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.io.Connection;
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
import org.everthrift.jetty.monitoring.RpsServlet;
import org.everthrift.jetty.transport.http.BinaryThriftServlet;
import org.everthrift.jetty.transport.http.JsonThriftServlet;
import org.everthrift.jetty.transport.http.PlainJsonThriftServlet;
import org.jminix.console.servlet.MiniConsoleServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.Environment;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MBeanServer;
import javax.servlet.MultipartConfigElement;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.ArrayBlockingQueue;

public class JettyServer implements SmartLifecycle {

    @Autowired
    private Environment env;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private JsonThriftServlet jsonThriftServlet;

    @Autowired
    private BinaryThriftServlet binaryThriftServlet;

    @Autowired
    private PlainJsonThriftServlet plainJsonThriftServlet;

    @Autowired
    private RpsServlet rpsServlet;

    @Autowired(required = false)
    @Qualifier("mbeanServer")
    private MBeanServer mbeanServer;

    private final Logger log = LoggerFactory.getLogger(JettyServer.class);

    private Server jettyServer;

    private XmlWebApplicationContext springWebApplicationContext;

    @Value("${jetty.web-context-xml:web-context.xml}")
    private String webContextXml;

    @Value("${jetty.web-xml:WEB-INF/web.xml}")
    private String webXml;

    @Value("${jetty.host}")
    private String jettyHost;

    @Value("${jetty.cors:false}")
    private boolean jettyCors;

    @Value("${jetty.port}")
    private String jettyPort;

    @Value("${jetty.ssl.port:443}")
    private String jettySslPort;

    @Value("${spring.http.multipart.max-file-size:-1}")
    private long maxFileSize;

    @Value("${spring.http.multipart.max-request-size:-1}")
    private long maxRequestSize;

    @Value("${spring.http.multipart.file-size-threshold:0}")
    private int fileSizeThreshold;

    @PostConstruct
    private void initJetty() {

        log.info("Starting jetty server on {}:{}", jettyHost, jettyPort);

        final int capacity = Integer.parseInt(env.getProperty("jetty.capacity", "6000"));
        final int maxThreads = Integer.parseInt(env.getProperty("jetty.maxThreads", "10"));
        final int minThreads = Integer.parseInt(env.getProperty("jetty.minThreads", "10"));
        final int idleTimeout = Integer.parseInt(env.getProperty("jetty.idleTimeout", "60000"));

        final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(capacity);

        final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout, queue);
        threadPool.setName("jetty");

        jettyServer = new Server(threadPool);

        if (mbeanServer != null) {
            final MBeanContainer mbContainer = new MBeanContainer(mbeanServer);
            jettyServer.addEventListener(mbContainer);
            jettyServer.addBean(mbContainer);
        }

        // Register loggers as MBeans
        jettyServer.addBean(Log.getLog());

        final HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        http_config.setSecurePort(Integer.parseInt(jettySslPort));
        http_config.setOutputBufferSize(1024 * 16);
        http_config.setRequestHeaderSize(8192);
        http_config.setResponseHeaderSize(8192);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);

        final ServerConnector http = new ServerConnector(jettyServer, new HttpConnectionFactory(http_config));
        http.setIdleTimeout(30000);

        http.addBean(new Connection.Listener() {
            @Override
            public void onOpened(Connection connection) {
                log.debug("Open connection: {}:{}", connection.getEndPoint()
                                                              .getRemoteAddress(), connection.getEndPoint()
                                                                                             .getLocalAddress());
            }

            @Override
            public void onClosed(Connection connection) {
                log.debug("Close connection: {}:{}, bytes in/out: {}/{}, messages in/out:{}/{}",
                          connection.getEndPoint().getRemoteAddress(),
                          connection.getEndPoint().getLocalAddress(),
                          connection.getBytesIn(),
                          connection.getBytesOut(),
                          connection.getMessagesIn(),
                          connection.getMessagesOut());
            }
        });
        http.setHost(jettyHost);
        http.setPort(Integer.parseInt(jettyPort));
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

                final ServerConnector https = new ServerConnector(jettyServer,
                                                                  new SslConnectionFactory(contextFactory, HttpVersion.HTTP_1_1
                                                                      .toString()),
                                                                  new HttpConnectionFactory(https_config));
                https.setHost(jettyHost);
                https.setPort(Integer.parseInt(jettySslPort));
                jettyServer.addConnector(https);
            } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IOException e) {

                throw new RuntimeException(e);
            }
        }

        final WebAppContext jettyContext = new WebAppContext();
        jettyContext.setContextPath("/");
        jettyContext.setResourceBase("webapp");
        jettyContext.setDescriptor(webXml);
        jettyContext.setConfigurationClasses(WebAppContext.getDefaultConfigurationClasses());
        jettyServer.setHandler(jettyContext);

        ((ConfigurableApplicationContext) context).getBeanFactory().registerSingleton("webAppContext", jettyContext);
        ((ConfigurableApplicationContext) context).getBeanFactory()
                                                  .registerSingleton("servletContext", jettyContext.getServletContext());

        if (isJMinixEnabled()) {
            final MiniConsoleServlet jminix = new MiniConsoleServlet();
            jettyContext.addServlet(new ServletHolder(jminix), "/jmx/*");
        }

        jettyContext.addServlet(new ServletHolder(jsonThriftServlet), "/TJSON");
        jettyContext.addServlet(new ServletHolder(binaryThriftServlet), "/TBINARY");

        final ServletHolder plainJsonThriftServletHolder = new ServletHolder(plainJsonThriftServlet);
        plainJsonThriftServletHolder.setAsyncSupported(true);
        jettyContext.addServlet(plainJsonThriftServletHolder, "/JSON/*");
        jettyContext.addServlet(new ServletHolder(rpsServlet), "/rps");

        springWebApplicationContext = new XmlWebApplicationContext();
        springWebApplicationContext.setParent(context);
        springWebApplicationContext.setConfigLocation(webContextXml);

        final DispatcherServlet dispatcherServlet = new DispatcherServlet(springWebApplicationContext);
        dispatcherServlet.setDispatchOptionsRequest(true);

        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);

        servletHolder.getRegistration().setMultipartConfig(new MultipartConfigElement(
            System.getProperty("java.io.tmpdir"),
            maxFileSize, maxRequestSize, fileSizeThreshold));

        jettyContext.addServlet(servletHolder, "/*");
        springWebApplicationContext.refresh();

        // hack for disable CORS
        for (AbstractHandlerMapping m : springWebApplicationContext.getBeansOfType(AbstractHandlerMapping.class)
                                                                   .values()) {
            m.setCorsProcessor((configuration, request, response) -> {
                if (jettyCors) {
                    response.addHeader("Access-Control-Allow-Origin", "*");
                    response.addHeader("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With, Content-Length, x-decompressed-content-length");
                    response.addHeader("Access-Control-Expose-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With, Content-Length, x-decompressed-content-length");
                }
                return true;
            });
        }

    }

    private KeyStore loadJettyKeystore(final String jks,
                                       String keystorePassword) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
        final InputStream inputStream = context.getResource(jks).getInputStream();
        final KeyStore keyStore;

        try {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(inputStream, keystorePassword != null ? keystorePassword.toCharArray() : null);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        return keyStore;
    }

    private boolean isJMinixEnabled() {
        return !env.getProperty("jminix", "false").equalsIgnoreCase("false");
    }

    @Override
    public void start() {
        try {
            jettyServer.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void stop() {
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            } catch (Exception e) {
            }
            jettyServer = null;
        }

        if (springWebApplicationContext != null) {
            try {
                springWebApplicationContext.close();
            } catch (Exception e) {
            }
            springWebApplicationContext = null;
        }
    }

    @PreDestroy
    private void destroy() {
        stop();
    }

    @Override
    public boolean isRunning() {
        return jettyServer != null && jettyServer.isRunning();
    }

    @Override
    public int getPhase() {
        return 100;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    public String getJettyHost() {
        return jettyHost;
    }

    public void setJettyHost(String jettyHost) {
        this.jettyHost = jettyHost;
    }

    public String getJettyPort() {
        return jettyPort;
    }

    public void setJettyPort(String jettyPort) {
        this.jettyPort = jettyPort;
    }

    public String getJettySslPort() {
        return jettySslPort;
    }

    public void setJettySslPort(String jettySslPort) {
        this.jettySslPort = jettySslPort;
    }
}
