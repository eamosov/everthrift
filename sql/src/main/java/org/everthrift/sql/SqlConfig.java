package org.everthrift.sql;

import net.sf.log4jdbc.sql.jdbcapi.DataSourceSpy;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.everthrift.sql.hibernate.LocalSessionFactoryBean;
import org.everthrift.sql.hibernate.model.MetaDataProvider;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class SqlConfig {

    @Bean
    public LocalSessionFactoryBean defaultSessionFactory(DataSource dataSource,
                                                         MetaDataProvider metaDataProvider,
                                                         @Value("${hibernate.dumphbm:false}") boolean dumpHbm,
                                                         @Value("${hibernate.statichbm:}") String staticHbm,
                                                         @Value("${hibernate.scan:}") String scan) throws IOException {

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

    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("defaultSessionFactory") SessionFactory sessionFactory) {
        final HibernateTransactionManager txManager = new HibernateTransactionManager();
        txManager.setSessionFactory(sessionFactory);
        return txManager;
    }

    @Bean
    public DataSource dataSource(@Value("${db.host}") String host,
                                 @Value("${db.name}") String db,
                                 @Value("${db.user}") String user,
                                 @Value("${db.pass}") String pass) {
        return new DataSourceSpy(setupPgDataSource(host, db, user, pass));
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    private DataSource setupPgDataSource(String host, String db, String user, String pass) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return setupDataSource(String.format("jdbc:postgresql://%s/%s?user=%s&password=%s", host, db, user, pass));
    }

    private DataSource setupDataSource(String connectURI) {

        final ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI, null);

        final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);

        poolableConnectionFactory.setValidationQuery("SELECT 1");

        final GenericObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        connectionPool.setTestOnBorrow(true);
        connectionPool.setMaxTotal(16);
        connectionPool.setMaxWaitMillis(5000);
        poolableConnectionFactory.setPool(connectionPool);
        PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);
        return dataSource;
    }

    @Bean
    public MetaDataProvider metaDataProvider(DataSource dataSource) throws IOException {
        final String config_file_location = "hiber_tables.properties";
        return new MetaDataProvider(dataSource, PropertiesLoaderUtils.loadAllProperties(config_file_location));
    }

    private Properties hibernateProperties() {
        return new Properties() {
            {
                setProperty("hibernate.hbm2ddl", "false");
                setProperty("hibernate.show_sql", "false");
                setProperty("hibernate.check_nullability", "false");
                setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQL82Dialect");
                setProperty("hibernate.format_sql", "true");
                setProperty("hibernate.cache.region.factory_class", "org.everthrift.sql.hibernate.HibernateCache");
                setProperty("hibernate.cache.use_second_level_cache", "true");
            }
        };
    }


}
