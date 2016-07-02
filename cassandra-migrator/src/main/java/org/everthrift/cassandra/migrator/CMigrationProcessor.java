package org.everthrift.cassandra.migrator;

import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import io.smartcat.migration.Migration;
import io.smartcat.migration.MigrationEngine;
import io.smartcat.migration.MigrationResources;

public class CMigrationProcessor implements ApplicationContextAware{

    private static final Logger log = LoggerFactory.getLogger(CMigrationProcessor.class);

    private ApplicationContext context;

    private Session session;
    private String schemaVersionCf = "schema_version";

    private String basePackage = "org.everthrift.cas.migration";

    private final MigrationResources resources = new MigrationResources();
    private final List<Migration> migrations = Lists.newArrayList();

    public CMigrationProcessor(){

    }

    private void findMigrations(String basePackage) {

        log.info("Using basePackage:{}", basePackage);

        final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(Migration.class));

        final DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)((ConfigurableApplicationContext)context).getBeanFactory();

        for (BeanDefinition b : scanner.findCandidateComponents(basePackage)) {

            final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());

            beanFactory.registerBeanDefinition(cls.getSimpleName(), BeanDefinitionBuilder.rootBeanDefinition(cls).getBeanDefinition());

            final Migration migration = context.getBean(cls.getSimpleName(), Migration.class);

            if (migration == null)
                throw new RuntimeException("cound't get bean:" + cls.getSimpleName());

            migrations.add(migration);
        }

    }

    public void migrate() throws Exception {

        Assert.notNull(session);
        Assert.notNull(context);

        findMigrations(basePackage);

        migrations.sort(new Comparator<Migration>(){

            @Override
            public int compare(Migration o1, Migration o2) {
                return Ints.compare(o1.getVersion(), o2.getVersion());
            }});
        resources.addMigrations(Sets.newLinkedHashSet(migrations));

        if (MigrationEngine.withSession(session, schemaVersionCf).migrate(resources) == false)
            throw new RuntimeException("Error in cassandra migrations");
    }

    public String getBasePackage() {
        return basePackage;
    }

    public void setBasePackage(String basePackage) {
        this.basePackage = basePackage;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getSchemaVersionCf() {
        return schemaVersionCf;
    }

    public void setSchemaVersionCf(String schemaVersionCf) {
        this.schemaVersionCf = schemaVersionCf;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

}
