package com.knockchat.sql.migration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;

import com.knockchat.utils.ConsoleUtils;
import com.knockchat.utils.logging.ColorOffConverter;
import com.knockchat.utils.logging.ColorOnConverter;

import ch.qos.logback.classic.PatternLayout;

public class Migrator {
	
	static {
		PatternLayout.defaultConverterMap.put( "coloron", ColorOnConverter.class.getName() );
		PatternLayout.defaultConverterMap.put( "coloroff", ColorOffConverter.class.getName() );		
	}			

    private static AbstractXmlApplicationContext context;
    private static ConfigurableEnvironment env;
    private static MigrationProcessor processor;
    
    private static final Logger log = LoggerFactory.getLogger(Migrator.class);

    /**
     *
     * @param args --force
     *             --name="name1, name2..." (name - Name of Migration)
     *             --up/--down (default: -up)
     *             --root.packages="x.y.z, a.b.c ..."
     **/
    public static void main(String[] args) {
        initContext(args);
        context.refresh();
        if (env.getProperty("help")!=null){
            printHelp();
        }else {
            processor = (MigrationProcessor) context.getBean("processor");
            
            final Map<String, MigrationProcessor.Result> result = processor.process(
                    env.getProperty("force") != null
                    , env.getProperty("name", List.class) == null ? Collections.emptyList() : env.getProperty("name", List.class)
                    , env.getProperty("down") != null);
            
            for (Map.Entry<String, MigrationProcessor.Result> entry : result.entrySet()){
            	
            	if (entry.getValue().equals(MigrationProcessor.Result.FAIL)){
            		log.error(entry.getKey(), entry.getValue().getException());	
            	}else{
            		log.info("{}\t\t{}", entry.getKey(), entry.getValue());
            	}
            	
                //ConsoleUtils.printString(entry.getKey() + "\t\t" + (entry.getValue().equals(MigrationProcessor.Result.FAIL)? entry.getValue().getMessage() : entry.getValue() ) + " \n");
            }
        }
    }

    private static void initContext(String[] args){
        context = new ClassPathXmlApplicationContext(new String[]{"classpath:migration-context.xml"},false);
        context.registerShutdownHook();
        env = context.getEnvironment();
        env.getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));
        env.getPropertySources().addLast(new MapPropertySource("scan.packages", Collections.singletonMap("root.packages", (Object) Arrays.asList("com.knockchat.sql.migration"))));
        final Resource resource = context.getResource("classpath:application.properties");
        try {
            env.getPropertySources().addLast(new ResourcePropertySource(resource));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        final Resource resource2 = context.getResource("classpath:php.properties");
        try {
            env.getPropertySources().addFirst(new ResourcePropertySource(resource2));
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    public static AbstractXmlApplicationContext getContext() {
        return context;
    }

    public static MigrationProcessor getProcessor() {
        return processor;
    }

    private static void printHelp(){
        ConsoleUtils.printString("\t\t\tMigrator Help \n");
        ConsoleUtils.printString("\tjava [opts] -jar migrator-version-with-deps.jar [args] \n");
        ConsoleUtils.printString("\t\t\t[opts] \n");
        ConsoleUtils.printString("-Dloader.path: \t List of locations appended to classpath. Default: lib(from main jar) " +
                "Include php.properties with DB connection details \n");
        ConsoleUtils.printString("\t\t\t[args] \n");
        ConsoleUtils.printString("\t--help \tThis help. \n");
        ConsoleUtils.printString("\t--force \tForce execute migrations \n");
        ConsoleUtils.printString("\t--name \tList of migrations to execute \n");
        ConsoleUtils.printString("\t--root.packages \tcomma separated list of packages to scan \n");
        ConsoleUtils.printString("\t--up/--down \tUp or Down migration (default:up) \n");
        ConsoleUtils.printString("\t\t\tExample: \n");
        ConsoleUtils.printString("\tjava -Dloader.path=\"lib, x.jar, y.jar, ./php.properties\" -jar migrator-0.0.1-with-deps.jar --name=\"Migartion1\" " +
                        " --root.packages=\"ru\" --down --force");
    }

}
