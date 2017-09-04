package org.everthrift.sql.migrator;

import com.google.common.primitives.Ints;
import org.everthrift.sql.io.smartcat.migration.SqlMigration;
import org.everthrift.sql.io.smartcat.migration.Migration;
import org.everthrift.sql.io.smartcat.migration.MigrationEngine;
import org.everthrift.sql.io.smartcat.migration.MigrationResources;
import org.everthrift.sql.io.smartcat.migration.MigrationType;
import org.everthrift.sql.io.smartcat.migration.exceptions.MigrationException;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlMigrationProcessor implements ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(SqlMigrationProcessor.class);

    private ApplicationContext context;

    private JdbcTemplate jdbcTemplate;

    private String schemaVersionCf = "schema_version";

    private String basePackage = "org.everthrift.sql.migration";

    private final MigrationResources resources = new MigrationResources();

    public SqlMigrationProcessor() {

    }

    private List<Migration> findMigrations(String basePackage) {

        log.info("Using basePackage:{}", basePackage);

        final List<Migration> migrations = new ArrayList<>();

        final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(Migration.class));

        final DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) ((ConfigurableApplicationContext) context)
            .getBeanFactory();

        for (BeanDefinition b : scanner.findCandidateComponents(basePackage)) {

            final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());

            beanFactory.registerBeanDefinition(cls.getSimpleName(), BeanDefinitionBuilder.rootBeanDefinition(cls)
                                                                                         .getBeanDefinition());

            final Migration migration = context.getBean(cls.getSimpleName(), Migration.class);

            if (migration == null) {
                throw new RuntimeException("cound't get bean:" + cls.getSimpleName());
            }

            migrations.add(migration);
        }

        return migrations;
    }

    private List<Migration> findCQLMigrations(final String rootResourcePath) throws URISyntaxException, IOException {
        final List<Migration> migrations = new ArrayList<>();

        for (String r : ResourceScanner.getInstance().getResources(rootResourcePath, Pattern.compile(".*\\.cql"),
                                                                   SqlMigrationProcessor.class.getClassLoader())) {
            migrations.add(new SqlMigration(MigrationType.SCHEMA, r));
        }

        return migrations;
    }

    private void assertUnique(List<Migration> migrations) throws MigrationException {
        final Set<Migration> set = new HashSet<>();
        for (Migration m : migrations) {
            if (!set.add(m)) {
                throw new MigrationException("Migration " + m.toString() + " is not unique");
            }
        }
    }

    public void migrate() throws Exception {
        migrate(m -> true, false);
    }

    public void migrate(MigrationType type, int version) throws Exception {
        migrate(m -> (type.equals(m.getType()) && m.getVersion() == version), false);
    }

    public void migrate(Predicate<Migration> filter, boolean force) throws MigrationException {
        Assert.notNull(jdbcTemplate);
        Assert.notNull(context);

        List<Migration> migrations = new ArrayList<>();
        migrations.addAll(findMigrations(basePackage));
        try {
            migrations.addAll(findCQLMigrations(basePackage.replaceAll("\\.", "/")));
        } catch (URISyntaxException | IOException e) {
            new MigrationException("Coudn't load CQL migrations", e);
        }

        migrations = migrations.stream().filter(filter).collect(Collectors.toList());
        assertUnique(migrations);
        migrations.sort((o1, o2) -> Ints.compare(o1.getVersion(), o2.getVersion()));

        resources.addMigrations(migrations);

        if (MigrationEngine.withJdbcTemplate(jdbcTemplate, schemaVersionCf).migrate(resources, force) == false) {
            throw new RuntimeException("Error in SQL migrations");
        }
    }

    public String getBasePackage() {
        return basePackage;
    }

    public void setBasePackage(String basePackage) {
        this.basePackage = basePackage;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
