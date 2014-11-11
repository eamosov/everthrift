package com.knockchat.utils.settings;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.knockchat.utils.EscapeChars;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class PersistsSettings {
	
	private static final Logger log = LoggerFactory.getLogger(PersistsSettings.class);
	static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	
	static HashMap<String, SettingsEditor> h = new HashMap<String, SettingsEditor>();
	static HashMap<String, Node> hn = new HashMap<String, Node>();
	static Object lock = new Object();
	
	public static void addSettings(Object o, String name)
			throws AllreadyExistEx {

		synchronized (lock) {

			SettingsEditor old = h.get(name);
			if (old != null) {
				throw new AllreadyExistEx("Settings " + name
						+ " allready exist", old.o);
			}

			SettingsEditor e = new SettingsEditor(o, name);
			h.put(name, e);

			Node n = hn.get(name);
			if (n != null) {
				hn.remove(name);
				
				log.info("restoring settings for {}", name);

				NamedNodeMap attr = n.getAttributes();

				for (int j = 0; j < attr.getLength(); j++) {
					Node pair = attr.item(j);
					log.debug("restore {} {}", pair.getNodeName(), pair.getTextContent());
					e.setValue(pair.getNodeName(), pair.getTextContent(), false);
				}
			}
		}
	}

	public static void removeSettings(String name) {
		synchronized (lock) {
			h.remove(name);
		}
	}

	public static String genXML(String name) {
		synchronized (lock) {
			StringBuilder out = new StringBuilder();
			
			SettingsEditor e = h.get(name);

			out.append("<");
			out.append(e.getName());
			out.append(" ");
			e.genXML(out);
			out.append("/>\n");
			return out.toString();
		}		
	}
	
	public static String genXML() {
		synchronized (lock) {

			StringBuilder out = new StringBuilder();

			out.append("<SETTINGS>\n");

			Set<String> keys = h.keySet();

			for (String key : keys) {
				out.append(genXML(key));
			}
			
			keys = hn.keySet();
			for (String key: keys ){
				Node n = hn.get(key);
				
				out.append("<");
				out.append(n.getNodeName());
				out.append(" ");
				
				log.debug("Saving not registered settings {}", n.getNodeName());
				NamedNodeMap map =  n.getAttributes();
				
				for(int i=0; i< map.getLength(); i++){
					Node attr = map.item(i);
					out.append(attr.getNodeName());
					out.append("=\"");
					out.append(EscapeChars.forXML(attr.getTextContent()));
					out.append("\" \n");
					//JNative.log("find attr " + attr.getNodeName() + "=" + attr.getTextContent());
				}
				
				out.append("/>\n");
			}

			out.append("</SETTINGS>\n");
			return out.toString();
		}
	}

	public static String genTypes() {
		synchronized (lock) {
			String out = new String();

			Set<String> keys = h.keySet();

			for (String key : keys) {
				SettingsEditor e = h.get(key);
				out += e.genTypes();
			}

			return out;
		}
	}

	public static String getValue(String var) throws NoSuchSettingEx {

		synchronized (lock) {
			// JNative.log(var);

			String[] tokens = var.split("\\.");

			if (tokens.length < 2)
				throw new NoSuchSettingEx("No such settings: " + var);

			// JNative.log(var + " " + tokens.length);
			String name = tokens[0];

			SettingsEditor e = h.get(name);
			if (e == null)
				throw new NoSuchSettingEx("No such settings: " + name);

			try {
				return e.getValue(var);
			} catch (SecurityException e1) {
				log.error("", e1);
				throw new NoSuchSettingEx("No such settings: " + var);
			} catch (IllegalArgumentException e1) {
				log.error("", e1);
				throw new NoSuchSettingEx("No such settings: " + var);
			} catch (NoSuchFieldException e1) {
				log.error("", e1);
				throw new NoSuchSettingEx("No such settings: " + var);
			} catch (IllegalAccessException e1) {
				log.error("", e1);
				throw new NoSuchSettingEx("No such settings: " + var);
			}
		}
	}

	public static boolean setValue(String var, String val, boolean hooks) {

		synchronized (lock) {

			String name = var.split("\\.")[0];

			SettingsEditor e = h.get(name);
			if (e == null) {
				log.warn("No such settings: {}", name);
				return false;
			}

			return e.setValue(var, val, hooks);
		}
	}

	public static void parseXML(String xml) throws ParseErrEx {

		synchronized (lock) {

			final Document doc;
			try {
				DocumentBuilder parser = factory.newDocumentBuilder();
				doc = parser.parse(new InputSource(new StringReader(xml)));
			} catch (SAXException e1) {
				log.error("", e1);
				throw new ParseErrEx(e1.getMessage());
			} catch (IOException e1) {
				log.error("", e1);
				throw new ParseErrEx(e1.getMessage());
			} catch (ParserConfigurationException e1) {
				log.error("", e1);
				throw new ParseErrEx(e1.getMessage());
			}
			
			NodeList nodes = doc.getElementsByTagName("*");
			//JNative.log("find " + nodes.getLength());

			for (int i = 1; i < nodes.getLength(); i++) {
				Node n = nodes.item(i);

				String name = n.getNodeName();

				SettingsEditor e = h.get(name);
				if (e == null) {
					log.warn("coudn't find settings: {}", name);
					hn.put(name, n); /* save it */
					continue;
				}

				NamedNodeMap attr = n.getAttributes();

				for (int j = 0; j < attr.getLength(); j++) {
					Node pair = attr.item(j);
					// JNative.log(pair.getNodeName() + pair.getTextContent());
					e.setValue(pair.getNodeName(), pair.getTextContent(), false);
				}

			}
		}
	}

}
