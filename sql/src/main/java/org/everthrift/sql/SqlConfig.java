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
import org.everthrift.sql.hibernate.model.types.CustomUserType;
import org.hibernate.SessionFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import java.util.Properties;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class SqlConfig {

    @NotNull
    @Bean
    public LocalSessionFactoryBean defaultSessionFactory(DataSource dataSource,
                                                         MetaDataProvider metaDataProvider,
                                                         @Value("${hibernate.dumphbm:false}") boolean dumpHbm,
                                                         @NotNull @Value("${hibernate.statichbm:}") String staticHbm,
                                                         @NotNull @Value("${hibernate.scan:}") String scan) throws IOException {

        final LocalSessionFactoryBean sessionFactory = new LocalSessionFactoryBean(dumpHbm);
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

    @NotNull
    @Bean
    public MetaDataProvider metaDataProvider(DataSource dataSource) throws IOException {

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

        final String config_file_location = "hiber_tables.properties";
        return new MetaDataProvider(dataSource, PropertiesLoaderUtils.loadAllProperties(config_file_location));
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
