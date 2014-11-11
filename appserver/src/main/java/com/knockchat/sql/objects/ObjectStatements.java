package com.knockchat.sql.objects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

import com.knockchat.sql.objects.basic.AbstractBasicStatement;
import com.knockchat.sql.objects.basic.BasicObjectStatementFactory;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
@Component
@ManagedResource(objectName="bean:name=ObjectStatements")
public class ObjectStatements {

	private static Logger log = LoggerFactory.getLogger(ObjectStatements.class); 
	//private static final LinkedList<ObjectStatementFactory> factories = new LinkedList<ObjectStatementFactory>();
	
	@Autowired
	private BasicObjectStatementFactory factory;

	private static final Pattern fieldWithTable =  Pattern.compile("^((\\w+)\\.){0,1}((\\w+\\$)*([\\w:]+))", Pattern.CASE_INSENSITIVE);
	private static final Pattern fieldWithAS =  Pattern.compile("(^.*)(\\b(\\w+)\\.)((\\w+\\$)*(\\w+))(.*)AS\\s+((\\w+\\$)*(\\w+))$", Pattern.CASE_INSENSITIVE);
	
	public <ObjectType> UpdateStatement<ObjectType> getUpdate( Class<ObjectType> objectClass, String sql, String... fields ) {
			return factory.getUpdate( objectClass, sql, fields );
	}

	public <ObjectType> QueryStatement<ObjectType> getQuery( Class<ObjectType> objectClass, String sql ) {
			return factory.getQuery( objectClass, sql );
	}

	public <ObjectType> UpdateStatement<ObjectType> getInsert( Class<ObjectType> objectClass, String table, String... fields ) {
		return factory.getInsert( objectClass, table, fields );
	}
	
	private static String trimColon(String as){		
		int colon= as.indexOf(":");
		if (colon != -1)
			as = as.substring(0, colon);
		return as;
	}
	
	public static String maskFields(final String fields, final String asPrefix, final String tableName, final String fieldPrefix){
		StringBuffer ret = new StringBuffer(fields.length());
		boolean first = true;
		

		int start = 0;
		int block=0;
		
		for (int i=0; i<fields.length(); i++){
			String f;
			
			switch (fields.charAt(i)){
			case '[':
			case '(':
			case '{':
				block++;
				continue;
			case ']':
			case ')':
			case '}':
				block--;
				continue;
			}
			
			boolean end =  i==(fields.length()-1);
			
			if ((fields.charAt(i)==',' || end) && block==0){
				f = fields.substring(start, end ? i+1 : i);
				start = i+1;
			}else{
				continue;
			}
			
			f = f.trim();
			
			if (!first) ret.append(",");
			
			first = false;
			
			Matcher m = fieldWithAS.matcher(f);
			if (m.matches()){
				//m.appendReplacement(ret, "");
				//ret.append(m.group(1) +  " AS " + prefix + "$" + m.group(4));
				//m.appendTail(ret);
				ret.append(m.group(1));

				if (tableName!=null)
					ret.append(tableName + ".");
					
				if (fieldPrefix !=null)
					ret.append(fieldPrefix + "$");
				
				ret.append(m.group(6));
				ret.append(m.group(7));
							
				if (asPrefix !=null)
					ret.append(" AS " + asPrefix + "$" + trimColon(m.group(10)));
				else
					ret.append(" AS " + trimColon(m.group(10)));
				
				continue;
			}
			
			m = fieldWithTable.matcher(f);
				
			if (m.matches()){
				//m.appendReplacement(ret, m.group(1) + "." + m.group(2));
				//m.appendTail(ret);
				//ret.append(f);
				if (tableName!=null)
				ret.append(tableName + ".");
				
				if (fieldPrefix !=null)
					ret.append(fieldPrefix + "$");
				
				if (asPrefix !=null)
					ret.append( m.group(5) +  " AS " + asPrefix + "$" + trimColon(m.group(5)));
				else
					ret.append( m.group(5) +  " AS " + trimColon(m.group(5)));
				
				continue;
			}
			
			final RuntimeException e = new RuntimeException("coudn't mask field '" + f + "' in fields '" + fields + "'");
			log.error("", e);
			throw e;
		}
		return ret.toString();
	}
	
	@ManagedOperation(description="getExecutionLog")
	public String getExecutionLog(){
		return AbstractBasicStatement.getExecutionLog();
	}

	@ManagedOperation(description="logExecutionLog")
	public void logExecutionLog(){
		log.info("\n{}", AbstractBasicStatement.getExecutionLog());
	}

	@ManagedOperation(description="resetExecutionLog")
	public void resetExecutionLog(){
		AbstractBasicStatement.resetExecutionLog();
	}

}