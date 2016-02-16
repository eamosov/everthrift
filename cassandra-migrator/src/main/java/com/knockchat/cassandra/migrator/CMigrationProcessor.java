package com.knockchat.cassandra.migrator;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.Session;

import io.smartcat.migration.Migration;
import io.smartcat.migration.MigrationEngine;
import io.smartcat.migration.MigrationResources;

public class CMigrationProcessor {
	
	private static final Logger log = LoggerFactory.getLogger(CMigrationProcessor.class);
	
    @Autowired
    private ConfigurableApplicationContext context;
    
    @Autowired
    private Session session;
    
    @Value("${cassandra.migrations.basePackage?:com.knockchat.cas.migration}")
    private String basePackage;
    
	private final MigrationResources resources = new MigrationResources();

	private void findMigrations(String basePackage) {
		
		log.info("Using basePackage:{}", basePackage);
		
		final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(Migration.class));

	    final DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)context.getBeanFactory();

        for (BeanDefinition b : scanner.findCandidateComponents(basePackage)) {
        	
            final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());
            	
           	beanFactory.registerBeanDefinition(cls.getSimpleName(), BeanDefinitionBuilder.rootBeanDefinition(cls).getBeanDefinition());
            	
           	final Migration migration = context.getBean(cls.getSimpleName(), Migration.class);
           	
           	if (migration == null)
           		throw new RuntimeException("cound't get bean:" + cls.getSimpleName());

            resources.addMigration(migration);
        }
		
	}
	
	@PostConstruct
    private void postConstruct() {               
		findMigrations(basePackage);
    }
    
    public boolean migrate(){
        return  MigrationEngine.withSession(session).migrate(resources);
    }

}
