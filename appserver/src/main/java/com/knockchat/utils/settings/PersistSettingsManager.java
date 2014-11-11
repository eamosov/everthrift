package com.knockchat.utils.settings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class PersistSettingsManager extends Thread {
	String path;
	boolean running=true;
	
	String lastXml;
	
	
	private final static Logger log = LoggerFactory.getLogger(PersistSettingsManager.class);

	public PersistSettingsManager(String aPath) {
		this.setName("Persist settings");
		path = aPath;
	}
	
	public void exit(){
		running = false;
		this.interrupt();
		try {
			this.join();
		} catch (InterruptedException e) {
			log.error("", e);
		}
	}
	
	public void loadSettings() throws ParseErrEx {
		synchronized (path) {
			loadSettings(path);
		}
	}

	public void loadSettings(String path) throws ParseErrEx {

		File to = new File(path);

		if (!(to.exists() && to.isFile() && to.canRead())) {
			log.error("coudn't load settings from file {}", to.getAbsolutePath());
			throw new ParseErrEx("coudn't load settings from file " + to.getAbsolutePath());
		}
		
		 FileInputStream r;

		try {
			r = new FileInputStream(to);
		} catch (FileNotFoundException e) {
			log.error("", e);
			log.error("coudn't load settings from file {}", to.getAbsolutePath());
			throw new ParseErrEx(e);
		}

		log.info("loading settings from {}", to.getAbsolutePath());

		byte[] cbuf = new byte[(int) to.length()];
		int ret;

		try {
			ret = r.read(cbuf);
		} catch (IOException e) {
			log.error("", e);
			try {
				r.close();
			} catch (IOException e1) {
				log.error("", e1);
			}
			log.error("coudn't read settings from file {}", to.getAbsolutePath());
			throw new ParseErrEx(e);
		}

		if (ret != (int) to.length()) {
			log.error("coudn't read {} chars, have read {}", to.length(), ret);
			try {
				r.close();
			} catch (IOException e) {
				log.error("", e);
			}
			throw new ParseErrEx("coudn't load settings from file " + to.getAbsolutePath());
		}

		String xml = new String(cbuf,Charset.forName( "UTF-8" ));

		// JNative.log("read " + ret + "chars: " + xml);

		try {
			PersistsSettings.parseXML(xml);
		} catch (ParseErrEx e) {
			log.error("", e);
			log.error("coudn't parse XML settings from file {}", to.getAbsolutePath());
			throw e;
		}
		
		lastXml = xml;
	}

	public void saveSettings(boolean force) {

		synchronized (path) {

			File tmp;

			String xml = PersistsSettings.genXML();
			
			if (xml.equals(lastXml) && !force)
				return;

			File dir = new File(".");
			try {
				tmp = File.createTempFile("setings-", ".zgs", dir);
			} catch (IOException e1) {
				log.error("coudn't create tmp file in dir {}",dir.getAbsolutePath());
				log.error("", e1);
				return;
			}
			
			try {
				final FileOutputStream out = new FileOutputStream(tmp);
				out.write(xml.getBytes());
				out.flush();
				out.getFD().sync();
				out.close();				
			}catch (FileNotFoundException e) {
				log.error("file not found", e);
				tmp.delete();
				return;				
			}catch (IOException e) {
				log.error("coudn't write to tmp file", e);
				tmp.delete();
				return;				
			}			

			final File to = new File(path);

			log.debug("saving settings to {}", to.getAbsolutePath());
			
			if (tmp.renameTo(to) == false) {
				tmp.delete();
				log.error("coudn't rename {} to {}", tmp.getAbsolutePath(), to.getAbsolutePath());
			}
		}
	}

	@Override
	public void run() {

		this.setName("Settings");

		log.info("running Persist Settings thread");

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			if(!running){
				log.debug("exiting");
				return;
			}
		}

		while (running) {
			
			//this.j

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				if(!running){
					log.debug("exiting");
					return;
				}
			}

			// JNative.log(5, "saving settings");

			// String xml = Container.genXML();

			saveSettings(false);
		}
		
		log.debug("exiting");
	}
}
