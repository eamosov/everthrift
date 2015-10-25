package com.knockchat.appserver.monitoring;

import gnu.trove.map.hash.TObjectLongHashMap;

import java.awt.Color;
import java.io.IOException;
import java.util.Date;

import javax.annotation.Resource;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdBackendFactory;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Sample;
import org.rrd4j.graph.RrdGraph;
import org.rrd4j.graph.RrdGraphDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import com.knockchat.utils.LongTimestamp;

@Component
public class RpsServlet extends HttpServlet implements InitializingBean, DisposableBean{
	
	private final static Logger log = LoggerFactory.getLogger(RpsServlet.class);

	private static final long serialVersionUID = 1L;
	
	public final int step = 5;

	public final int arch1_size = 3600*24;	// 24 hour
	public final int arch1_step = 5;		// 5 seconds average

	public final int arch2_size = 3600 * 24 * 7; // week
	public final int arch2_step = 60;		 	 // minute average
	
	private RrdDb rrdDb;
	private Sample sample;
	
	private final TObjectLongHashMap<DsName> counters = new TObjectLongHashMap<DsName>();
	
	public static enum DsName{
		
		THRIFT_TCP("th_tcp"),
		THRIFT_HTTP("th_http"),
		THRIFT_JGROUPS("th_jgroups"),
		THRIFT_JMS("th_jms"),
		THRIFT_WS("th_ws");
		
		private final String dsName;
		
		DsName(String dsName){
			this.dsName = dsName;
		}
	}
	
	@Resource
	private TaskScheduler myScheduler;

	public RpsServlet() {

	}

	@Override
	public void afterPropertiesSet() throws Exception {

		final RrdDef rrdDef = new RrdDef("rps", System.currentTimeMillis() / 1000, step);
		rrdDef.setVersion(2);
		
		rrdDef.addArchive(ConsolFun.AVERAGE, 0.5, arch1_step / step, arch1_size / arch1_step);
		rrdDef.addArchive(ConsolFun.AVERAGE, 0.5, arch2_step / step, arch2_size / arch2_step);

		for (DsName n : DsName.values()){
			counters.put(n, 0);
			rrdDef.addDatasource(n.dsName, DsType.COUNTER, step * 2, 0, Long.MAX_VALUE);
		}
						 
		rrdDb = new RrdDb(rrdDef, RrdBackendFactory.getFactory("MEMORY"));
		sample = rrdDb.createSample();
		
    	myScheduler.scheduleAtFixedRate(this::sample, new Date(System.currentTimeMillis() + step * 1000), step * 1000L);		
	}
	
	private synchronized void sample(){
		sample.setTime(System.currentTimeMillis() / 1000);
		
		counters.forEachEntry((DsName n, long c) -> { sample.setValue(n.dsName, c) ;return true;});
				
		try {
			sample.update();
		} catch (Exception e) {
			log.error("Exception in sample()", e);
		}		    
	}

	public void incThrift(DsName dsName){
		counters.increment(dsName);
	}
	
	@Override
	public void destroy() {
		
		super.destroy();
		
		try {
			rrdDb.close();
		} catch (IOException e) {
		}
	}
	
	private byte[] makeGraph(DsName dsName, long startMillis, long endMillis, int width, int height) throws IOException{

		final RrdGraphDef gDef = new RrdGraphDef();
		gDef.setWidth(width);
		gDef.setHeight(height);
		gDef.setStartTime(startMillis / 1000L);
		gDef.setEndTime(endMillis / 1000L);
		gDef.setTitle("Requests per second (" + dsName + ")");
		gDef.setImageFormat("png");

		gDef.datasource("thrift", rrdDb.getPath(), dsName.dsName, ConsolFun.AVERAGE, "MEMORY");
		
		gDef.line("thrift", Color.GREEN, "thrift requests");
		gDef.gprint("thrift", ConsolFun.LAST, "cur = %.3f%s");
		gDef.gprint("thrift", ConsolFun.AVERAGE, "avr = %.3f%s");
		gDef.gprint("thrift", ConsolFun.MAX, "max = %.3f%s\\c");		

		final RrdGraph graph = new RrdGraph(gDef);
				
		return graph.getRrdGraphInfo().getBytes();
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse response) throws IOException{
		
		response.setContentType("text/html");
		
		final String resource = req.getParameter("ds");
		final DsName dsName;
		if (resource == null)
			dsName = DsName.THRIFT_WS;
		else
			dsName = DsName.valueOf(resource);
		
		if (dsName == null)
			throw new RuntimeException("resource " + dsName + " not found");

		long now = LongTimestamp.now();
		
		final String min = String.format("<img alt=\"Requests per second %s\" src=\"data:image/png;base64,%s\"/>", dsName.name(), Base64.encodeBase64String(makeGraph(dsName, now - LongTimestamp.MIN * 10, now, 800, 400)));
		final String hour = String.format("<img alt=\"Requests per second %s\" src=\"data:image/png;base64,%s\"/>", dsName.name(), Base64.encodeBase64String(makeGraph(dsName, now - LongTimestamp.HOUR, now, 800, 400)));
		final String day = String.format("<img alt=\"Requests per second %s\" src=\"data:image/png;base64,%s\"/>", dsName.name(), Base64.encodeBase64String(makeGraph(dsName, now - LongTimestamp.DAY, now, 800, 400)));
		final String week = String.format("<img alt=\"Requests per second %s\" src=\"data:image/png;base64,%s\"/>", dsName.name(), Base64.encodeBase64String(makeGraph(dsName, now - LongTimestamp.WEEK, now, 800, 400)));
		
		final StringBuilder body = new StringBuilder();
		body.append("<html><head><title>" + dsName.name() + "</title></head><body>\n");
		
		for (DsName n : DsName.values()){
			body.append(String.format("<a href=\"?ds=%s\">%s</a>&nbsp&nbsp", n.name(), n.name()));
		}
		
		body.append("\n<br/>\n");
		
		body.append(min + "\n<br/>\n" + hour + "\n<br/>\n" + day + "\n<br/>\n" + week + "\n<br/>\n");
		body.append("</body></html>");
		
		final byte[] _body = body.toString().getBytes();
		
		response.setContentLength(_body.length);
		response.getOutputStream().write(_body);
		response.flushBuffer();			
	}
}
