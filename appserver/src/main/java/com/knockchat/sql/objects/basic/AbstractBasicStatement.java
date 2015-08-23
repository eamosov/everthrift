package com.knockchat.sql.objects.basic;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.Lists;
import com.knockchat.sql.SqlUtils;
import com.knockchat.utils.ExecutionStats;
import com.knockchat.utils.Pair;
import com.knockchat.utils.timestat.StatFactory;
import com.knockchat.utils.timestat.TimeStat;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public abstract class AbstractBasicStatement<ObjectType> {
	
	private static final Logger log = LoggerFactory.getLogger( AbstractBasicStatement.class );

	protected static final TimeStat ts;
	
	public static class Settings{
		public int debugLimitMcs = 0;
		public int infoLimitMcs = 0;
		public int warnLimitMcs = 0;
		//public int commitDelay = 100000;
		//public boolean logHistory = false;
	}

	public static final Settings settings = new Settings();

	static {
		try {
			ts = StatFactory.createTimeStat( "sql-query" );
		} catch ( Throwable e ) {
			throw new RuntimeException( "Error creating timestat 'sql-query'", e );
		}		
	}

	private static final Pattern cutFromPat = Pattern.compile("SELECT (.(?<!from)(?<!select))* FROM", Pattern.CASE_INSENSITIVE);
	public static final List<WeakReference<AbstractBasicStatement<?>>> statementList = Collections.<WeakReference<AbstractBasicStatement<?>>>synchronizedList((Lists.<WeakReference<AbstractBasicStatement<?>>>newLinkedList()));

	protected final String sql;
	protected final JdbcTemplate jdbcTemplate;
	
	protected final String fileName;
	protected final int lineNumber;

	private ExecutionStats stats = new ExecutionStats();

	public static void cleanupStatementList(boolean reset) {
		
		synchronized(statementList){
			ListIterator<WeakReference<AbstractBasicStatement<?>>> it = statementList.listIterator();
			
			while ( it.hasNext() ){
				AbstractBasicStatement<?> st = it.next().get();
				if ( st == null ){
					it.remove();
				}else if (reset){
					st.resetExecutionStats();
				}
			}
		}
	}

	public AbstractBasicStatement(JdbcTemplate jdbcTemplate, String sql) {
		this.sql = sql;
		this.jdbcTemplate = jdbcTemplate;
		
        Throwable e = new Throwable();
        StackTraceElement[] t = e.getStackTrace();
       	
       	if (t == null || t.length < 5){
       		fileName = "<unknown>";
       		lineNumber = 0;
       	}else if (t[3].getFileName() !=null && t[3].getFileName().equals("ObjectStatements.java")){
    		fileName = t[4].getFileName();
    		lineNumber = t[4].getLineNumber();
       	}else{
       		String fileName = "<unknown>";
       		int lineNumber = 0;
       		for (int i=0; i<t.length; i++){
       			if (t[i].getFileName() !=null && t[i].getFileName().equals("ObjectStatements.java") && (i+1) < t.length){
       				fileName = t[i+1].getFileName();
       				lineNumber = t[i+1].getLineNumber();
       				break;
       			}
       		}
       		this.fileName = fileName;
       		this.lineNumber = lineNumber;
       	}
		
		statementList.add( new WeakReference<AbstractBasicStatement<?>>( this ) );
	}

	public final String getSql() {
		return sql;
	}
	
	public final String getFileName(){
		return fileName == null ? "<nofile>" : fileName;
	}
	
	public final int getLineNumber(){
		return lineNumber;
	}

	public final ExecutionStats getExecutionStats() {
		return stats;
	}

	public  synchronized final void resetExecutionStats() {
		stats = new ExecutionStats();
	}

	protected synchronized final void updateExecutionStats( long time ) {
		stats = new ExecutionStats( stats, time );
	}

	protected final String quote( Object o ) {
		if ( o == null )
			return "null";

		return "'" + SqlUtils.toSqlParam( o.toString() ) + "'";
	}
	
	public static void resetExecutionLog(){
		AbstractBasicStatement.cleanupStatementList(true);
	}

	public static String getExecutionLog() {
		cleanupStatementList(false);

		final ArrayList<Pair<String,ExecutionStats>> list = new ArrayList<Pair<String,ExecutionStats>>( AbstractBasicStatement.statementList.size() );

		synchronized(statementList){				
			for ( WeakReference<AbstractBasicStatement<?>> ref : statementList ) {
				AbstractBasicStatement<?> st = ref.get();

				if ( st != null ) {
					list.add( new Pair<String,ExecutionStats>( cutFromQuery(st.getSql()) + ", " + st.getFileName() + ":" + st.getLineNumber(), st.getExecutionStats() ) );
				}
			}				
		}

		Collections.sort( list, new Comparator<Pair<String,ExecutionStats>>(){

			@Override
			public int compare( Pair<String, ExecutionStats> o1, Pair<String, ExecutionStats> o2 ) {
				return Long.signum( o2.second.getSummaryTime() - o1.second.getSummaryTime() );
			}
		} );

		return ExecutionStats.getLogString(list);
	}

	private static String cutFromQuery(String query){
		return cutFromPat.matcher(query).replaceAll("SELECT ... FROM");
	}	
}