package com.knockchat.sql.migration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;

import com.knockchat.utils.ConsoleUtils;

public class MigrationProcessor{

    @Autowired
    private PlatformTransactionManager txManager;

    @Autowired
    private ConfigurableApplicationContext context;

    public static final String GET_EXIST_MIGRATIONS_FROM_DB_QUERY = "select * from yii_migration where version in ( :versions ) ";
    private static Comparator<String> BY_VERSION_DATETIME = new Comparator<String>() {
        SimpleDateFormat sdf = new SimpleDateFormat("'m'yyMMdd_HHmmss");

        @Override
        public int compare(String o1, String o2) {
            long l1, l2 = 0;
            try {
                l1 = sdf.parse(o1.substring(0, 14)).getTime() / 1000;
            } catch (ParseException e) {
                l1 = Long.MAX_VALUE;
            }
            try {
                l2 = sdf.parse(o2.substring(0, 14)).getTime() / 1000;
            } catch (ParseException e) {
                l1 = Long.MAX_VALUE;
            }
            return Long.compare(l1, l2);
        }
    };

    private JdbcTemplate jdbcTemplate;
    private Map<String, com.knockchat.sql.migration.AbstractMigration> availableMigrations = new TreeMap(MigrationProcessor.BY_VERSION_DATETIME);

    public MigrationProcessor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String,Result> process(boolean force, List<String> migrationNames, boolean down) {
        findMigrations(migrationNames);
        if (down) {
            retainExistedInDb(new ArrayList(availableMigrations.keySet()));
        } else {
            excludeExistingInDb(new ArrayList(availableMigrations.keySet()));
        }
        return executeMigrations(force, migrationNames.isEmpty() ? false : down);

    }

    private void findMigrations(List<String> migrationNames) {
        boolean byName = !migrationNames.isEmpty();
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Migration.class));
        final List<String> l = context.getEnvironment().getProperty("root.packages", List.class);
        for (String s : l) {
            Map<String, AbstractMigration> migrationsByName = scanMigrationClasses(scanner, s);
            if (byName) {
                for (String name : migrationNames)
                    if (migrationsByName.containsKey(name))
                        availableMigrations.put(name, migrationsByName.get(name));
            } else {
                availableMigrations.putAll(migrationsByName);
            }
        }
    }

    private Map<String, AbstractMigration> scanMigrationClasses(ClassPathScanningCandidateComponentProvider scanner, String basePath) {
    	
        final Map<String, AbstractMigration> migrations4Path = new HashMap<>();
        final DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)context.getBeanFactory();
        
        for (BeanDefinition b : scanner.findCandidateComponents(basePath)) {
        	
            final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());
            	
           	beanFactory.registerBeanDefinition(cls.getSimpleName(), BeanDefinitionBuilder.rootBeanDefinition(cls).getBeanDefinition());
            	
           	final AbstractMigration migration = context.getBean(cls.getSimpleName(), AbstractMigration.class);
           	
           	if (migration == null)
           		throw new RuntimeException("cound't get bean:" + cls.getSimpleName());
           	
            migration.setJdbcTemplate(jdbcTemplate);
            migrations4Path.put(((Migration)cls.getAnnotation(Migration.class)).version(), migration);
        }
        return migrations4Path;
    }

    public enum Result {
        SUCCESS, FAIL, SKIP;

        private String message;

        public Result setMessage(String message) {
            this.message = message;
            return this;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "[" + this.name() + "] " + (message != null ? message : "");
        }
    }

    private Map<String,Result> executeMigrations(boolean force, boolean down) {
        Map<String,Result> migrationResults = new HashMap<>();
        for (String key : availableMigrations.keySet()) {
            ConsoleUtils.printString("%s \n", key);
        }
        ConsoleUtils.printString("Found %d new migrations.\n", availableMigrations.size());
        if (!force) {
            if (availableMigrations.size() > 0) {
                force = ConsoleUtils.readYN("Process? [Y/N]. Default: No. \n");
            }
        }
        if (force) {
            for (Map.Entry<String, AbstractMigration> entry : availableMigrations.entrySet()) {
                try {
                    //ConsoleUtils.printString(entry.getKey() + "\t\t" + executeMigration(entry, down) + " \n");
                    migrationResults.put(entry.getKey(),executeMigration(entry, down));
                } catch (Exception e) {
                    //ConsoleUtils.printString(entry.getKey() + "\t\t" + Result.FAIL.setMessage(e.getMessage()) + " \n");
                    migrationResults.put(entry.getKey(),Result.FAIL.setMessage(e.getMessage()));
                    break;
                }
            }
        }
        return migrationResults;
    }

    private Result executeMigration(final Map.Entry<String, AbstractMigration> entry, final boolean down) throws Exception {
        final TransactionTemplate txTemplate = new TransactionTemplate(txManager);
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                if (down) {
                    entry.getValue().down();
                    entry.getValue().deleteRowInMigrationTbl();
                } else {
                    entry.getValue().up();
                    entry.getValue().createRowInMigrationTbl();
                }
            }
        });
        return Result.SUCCESS;
    }

    private void excludeExistingInDb(List<String> migrationVersions) {
        if (migrationVersions.size() <= 0) {
            return;
        }
        NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        namedJdbcTemplate.query(MigrationProcessor.GET_EXIST_MIGRATIONS_FROM_DB_QUERY, Collections.singletonMap("versions", migrationVersions), new RowMapper<Object>() {
            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                return availableMigrations.remove(rs.getString("version"));
            }
        });
    }

    private void retainExistedInDb(final List<String> migrationVersions) {
        if (migrationVersions.size() <= 0) {
            return;
        }
        final Map<String, com.knockchat.sql.migration.AbstractMigration> tmpMigrations = new TreeMap(MigrationProcessor.BY_VERSION_DATETIME);
        tmpMigrations.putAll(availableMigrations);
        availableMigrations.clear();
        final NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        namedJdbcTemplate.query(MigrationProcessor.GET_EXIST_MIGRATIONS_FROM_DB_QUERY, Collections.singletonMap("versions", migrationVersions), new RowMapper<Object>() {
            @Override
            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                return availableMigrations.put(rs.getString("version"), tmpMigrations.get(rs.getString("version")));
            }
        });
    }
}
