package org.everthrift.cassandra.migrator;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;

import ch.qos.logback.classic.PatternLayout;

public class Migrator {

	static {
		PatternLayout.defaultConverterMap.put( "coloron", ColorOnConverter.class.getName() );
		PatternLayout.defaultConverterMap.put( "coloroff", ColorOffConverter.class.getName() );		
	}			

    private static AbstractXmlApplicationContext context;
    private static ConfigurableEnvironment env;
    private static CMigrationProcessor processor;
    
    private static final Logger log = LoggerFactory.getLogger(Migrator.class);

	public static void main(String[] args) throws IOException {
        context = new ClassPathXmlApplicationContext(new String[]{"classpath:cassandra-migration-context.xml"},false);
        context.registerShutdownHook();
        env = context.getEnvironment();
        env.getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));
        final Resource resource = context.getResource("classpath:application.properties");
        try {
            env.getPropertySources().addLast(new ResourcePropertySource(resource));
        } catch (IOException e) {
            log.error("Error loading resource: " + resource.toString(), e);
        }
        
        context.refresh();
        processor = context.getBean(CMigrationProcessor.class);
        
        processor.migrate();

        context.close();
	}

}
