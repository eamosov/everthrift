package com.knockchat.node;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.AbstractDataSource;

import ch.qos.logback.classic.PatternLayout;

import com.knockchat.appserver.AppserverApplication;
import com.knockchat.utils.logging.ColorOffConverter;
import com.knockchat.utils.logging.ColorOnConverter;

@Configuration
public class InfonodeApplication {
	
	static {
		PatternLayout.defaultConverterMap.put( "coloron", ColorOnConverter.class.getName() );
		PatternLayout.defaultConverterMap.put( "coloroff", ColorOffConverter.class.getName() );		
	}	
	
	@Bean
	public ScanConfigurationTask scanConfigurationTask(){
		return new ScanConfigurationTask();
	}
	
    @Bean
    public DataSource dataSource() {
    	return new AbstractDataSource(){

			@Override
			public Connection getConnection() throws SQLException {
				return null;
			}

			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return null;
			}};
    }
	
	private static final Logger log = LoggerFactory.getLogger(InfonodeApplication.class);

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(final String[] args) throws Exception {
		
		final Launcher l = new Launcher();
		
		l.init(new DaemonContext(){

			@Override
			public DaemonController getController() {
				return null;
			}

			@Override
			public String[] getArguments() {
				return args;
			}});
		
		l.start();

		AppserverApplication.INSTANCE.waitExit();
	}

	
}
