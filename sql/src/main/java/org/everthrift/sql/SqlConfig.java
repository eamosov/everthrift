package org.everthrift.sql;

import com.google.common.base.Throwables;
import net.sf.log4jdbc.sql.jdbcapi.DataSourceSpy;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.everthrift.sql.hibernate.LocalSessionFactoryBean;
import org.everthrift.sql.hibernate.model.CustomTypesRegistry;
import org.everthrift.sql.hibernate.model.MetaDataProvider;
import org.everthrift.sql.hibernate.model.MetaDataProviderIF;
import org.everthrift.sql.hibernate.model.types.CustomUserType;
import org.hibernate.SessionFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class SqlConfig {

    private final static Logger log = LoggerFactory.getLogger(SqlConfig.class);

    @NotNull
    @Bean
    public LocalSessionFactoryBean defaultSessionFactory(DataSource dataSource,
                                                         MetaDataProviderIF metaDataProvider,
                                                         @NotNull @Value("${hibernate.statichbm:}") String staticHbm,
                                                         @NotNull @Value("${hibernate.scan:}") String scan) throws IOException {

        final LocalSessionFactoryBean sessionFactory = new LocalSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);
        sessionFactory.setHibernateProperties(hibernateProperties());
        sessionFactory.setMetaDataProvider(metaDataProvider);

        if (!staticHbm.isEmpty()) {
            sessionFactory.setMappingResources(staticHbm.split(","));
        }

        if (!scan.isEmpty()) {
            sessionFactory.setPackagesToScan(scan.split(","));
        }

        return sessionFactory;
    }

    @NotNull
    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("defaultSessionFactory") SessionFactory sessionFactory) {
        final HibernateTransactionManager txManager = new HibernateTransactionManager();
        txManager.setSessionFactory(sessionFactory);
        return txManager;
    }

    @NotNull
    @Bean
    public DataSource dataSource(@Value("${db.host}") String host,
                                 @Value("${db.name}") String db,
                                 @Value("${db.user}") String user,
                                 @Value("${db.pass}") String pass) {
        return new DataSourceSpy(setupPgDataSource(host, db, user, pass));
    }

    @NotNull
    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Nullable
    private DataSource setupPgDataSource(String host, String db, String user, String pass) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return setupDataSource(String.format("jdbc:postgresql://%s/%s?user=%s&password=%s", host, db, user, pass));
    }

    @Nullable
    private DataSource setupDataSource(String connectURI) {

        final ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI, null);

        final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);

        poolableConnectionFactory.setValidationQuery("SELECT 1");

        final GenericObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        connectionPool.setTestOnBorrow(true);
        connectionPool.setMaxTotal(30);
        connectionPool.setMaxWaitMillis(5000);
        poolableConnectionFactory.setPool(connectionPool);
        PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);
        return dataSource;
    }

    public void registerCustomUserTypes() {

        final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(CustomUserType.class));

        for (BeanDefinition b : scanner.findCandidateComponents("org.everthrift.sql.hibernate.model.types")) {
            final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());
            if (!Modifier.isAbstract(cls.getModifiers())) {
                try {
                    CustomTypesRegistry.getInstance().register((CustomUserType) cls.newInstance());
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    @NotNull
    @Bean
    public MetaDataProviderIF metaDataProvider(DataSource dataSource, @Value("${hibernate.dumphbm:false}") boolean dumpHbm) throws IOException {

        registerCustomUserTypes();

        final Path path = Paths.get("_hbm_.xml");
        
        final String config_file_location = "hiber_tables.properties";
        final MetaDataProviderIF provider = new MetaDataProvider(dataSource, PropertiesLoaderUtils.loadAllProperties(config_file_location));

        if (dumpHbm) {
            try {
                log.info("dumping hbm.xml to {}", path);
                Files.write(path,
                            provider.getHbmXml().getBytes(),
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return provider;
    }

    @NotNull
    private Properties hibernateProperties() {
        return new Properties() {
            {
                setProperty("hibernate.hbm2ddl", "false");
                setProperty("hibernate.show_sql", "false");
                setProperty("hibernate.check_nullability", "false");
                //setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQL82Dialect");
                setProperty("hibernate.dialect", "org.hibernate.spatial.dialect.postgis.PostgisDialect");
                setProperty("hibernate.format_sql", "true");
                setProperty("hibernate.cache.region.factory_class", "org.everthrift.sql.hibernate.HibernateCache");
                setProperty("hibernate.cache.use_second_level_cache", "true");
            }
        };
    }


}
