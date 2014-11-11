package com.knockchat.node;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import ch.qos.logback.classic.PatternLayout;

import com.knockchat.appserver.AppserverApplication;
import com.knockchat.utils.logging.ColorOffConverter;
import com.knockchat.utils.logging.ColorOnConverter;

public class Launcher implements Daemon {

	static {
		PatternLayout.defaultConverterMap.put( "coloron", ColorOnConverter.class.getName() );
		PatternLayout.defaultConverterMap.put( "coloroff", ColorOffConverter.class.getName() );		
	}	

	public Launcher() {

	}

	@Override
	public void destroy() {

	}

	@Override
	public void init(DaemonContext arg0) throws DaemonInitException, Exception {
		AppserverApplication.INSTANCE.addScanPath("com.knockchat.node");
		AppserverApplication.INSTANCE.init(arg0.getArguments(), InfonodeApplication.class.getPackage().getImplementationVersion());
	}
	
	@Override
	public void start() throws Exception {
        AppserverApplication.INSTANCE.start();		
	}

	@Override
	public void stop() throws Exception {
		AppserverApplication.INSTANCE.stop();		
	}

}
