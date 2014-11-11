package com.knockchat.sql.inherited;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.sql.objects.ObjectStatements;
import com.knockchat.sql.objects.QueryStatement;

public class InheritanceManager implements InitializingBean {
	
	private static final Logger log = LoggerFactory.getLogger(InheritanceManager.class);
	
	@Autowired
	private ObjectStatements objectStatements;
	
	private QueryStatement<String> findTables;
	private QueryStatement<String> getConstraint;
	private final String tableName;
	
	protected final Pattern tablePattern;
	
	
	public InheritanceManager(String tableName){
		this.tableName= tableName;
		tablePattern = Pattern.compile(tableName + "_(\\d{4})_(\\d\\d)_(\\d\\d)");
	}
	
	protected String getOldTableName(){
		return tableName + "_old";
	}
	
	protected String getSampleTableName(){
		return getOldTableName();
	}
	
	protected String getConstraintName(){
		return tableName + "_check";
	}
	
	protected String getTriggerName(){
		return tableName + "_insert_trigger";
	}
	
	protected String getTriggerHeader(){
		return "\tNEW.id = nextval('" + tableName + "_id_seq');\n";
	}
	
	protected String getTriggerCondition(String table){
		return "NEW.updated_at <= " + getTableTimestamp(table);
	}

	protected String getTriggerLastCondition(String table){
		return "NEW.updated_at > " + getTableTimestamp(table);
	}
	
	protected String getFirstConstraint(String next){
		return "(updated_at <= " + getTableTimestamp(next) + ")";
	}
	
	protected String getMiddleConstraint(String cur, String next){
		return "((updated_at > " + getTableTimestamp(cur) + ") AND (updated_at <= " + getTableTimestamp(next) + "))";
	}
	
	protected String getLastConstraint(String cur){
		return "(updated_at > " + getTableTimestamp(cur) + ")";
	}

	public void chechParts(Connection c, boolean commit) throws SQLException{
		final List<String> tables = findTables.queryList(c, tableName);
	
		Collections.<String>sort(tables, new Comparator<String>(){

			@Override
			public int compare(String o1, String o2) {				
				return Integer.compare(getTableTimestamp(o1), getTableTimestamp(o2));
			}});

		final Calendar nowCld = Calendar.getInstance();
		final Calendar needCld = Calendar.getInstance();
		needCld.setTimeZone(TimeZone.getTimeZone("GMT"));
		needCld.set(nowCld.get(Calendar.YEAR), nowCld.get(Calendar.MONTH), 1, 0, 0, 0);
		needCld.add(Calendar.MONTH, 2);
		
		final String lastTable = tables.get(tables.size()-1);
		final Matcher m = tablePattern.matcher(lastTable);
		if (!m.matches())
			throw new RuntimeException("invalid table name: " + lastTable);
		
		final Calendar cld = Calendar.getInstance();
		cld.setTimeZone(TimeZone.getTimeZone("GMT"));
		cld.set(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))-1, Integer.parseInt(m.group(3)), 0, 0, 0);

		final StringBuilder sql = new StringBuilder();
		
		while (cld.getTimeInMillis() < needCld.getTimeInMillis()){
			cld.add(Calendar.MONTH, 1);
			
			final String nextTable = String.format("%s_%04d_%02d_%02d", tableName, cld.get(Calendar.YEAR), cld.get(Calendar.MONTH)+1, cld.get(Calendar.DAY_OF_MONTH));
			tables.add(nextTable);
			
			sql.append(makeTable(getSampleTableName(), nextTable));
			sql.append("\n");			
		}
		
		final Statement s  = c.createStatement();
		final String newTables = sql.toString();
		log.debug("NEW TABLES:\n{}", newTables);
		if (commit)
			s.execute(newTables);

		final String triggers = makeInsertTrigger(tables);
		log.debug("TRIGGER:\n{}", triggers);
		if (commit)
			s.execute(triggers);
		
		final String constraints = makeConstraints(c, tables);
		log.debug("CONSTRAINS:\n{}", constraints);
		if (commit)
			s.execute(constraints);
		
		s.close();
		
		if (!commit)
			c.rollback();		
	}
	
	public int getTableTimestamp(String table){
		if (table.equals(getOldTableName()))
			return 0;
		
		final Matcher m = tablePattern.matcher(table);
		if (!m.matches())
			throw new RuntimeException("invalid table name: " + table);
		
		final Calendar cld = Calendar.getInstance();
		cld.setTimeZone(TimeZone.getTimeZone("GMT"));
		cld.set(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))-1, Integer.parseInt(m.group(3)), 0, 0, 0);
		return (int)(cld.getTimeInMillis() / 1000);
	}
	
	/*public void makeNextPart(Connection c, int parts) throws SQLException{
		final List<String> tables = findTables.queryList(c, tableName);
		
		final StringBuilder sql = new StringBuilder();
		
		Collections.<String>sort(tables, new Comparator<String>(){

			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(getTableTimestamp(o1), getTableTimestamp(o2));
			}});
		
		
		for (int i=0; i<parts; i++){
			final String lastTable = tables.get(tables.size()-1);
			final Matcher m = tablePattern.matcher(lastTable);
			if (!m.matches())
				throw new RuntimeException("invalid table name: " + lastTable);
			
			final Calendar cld = GregorianCalendar.getInstance();
			cld.setTimeZone(TimeZone.getTimeZone("GMT"));
			cld.set(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))-1, Integer.parseInt(m.group(3)), 0, 0, 0);
			cld.add(Calendar.MONTH, 1);
			
			final String nextTable = String.format("%s_%04d_%02d_%02d", tableName, cld.get(Calendar.YEAR), cld.get(Calendar.MONTH)+1, cld.get(Calendar.DAY_OF_MONTH));
			tables.add(nextTable);
			
			sql.append(makeTable(getSampleTableName(), nextTable));
			sql.append("\n");
			
		}
		
		final Statement s  = c.createStatement();
		
		s.execute(sql.toString());
		
		sql.setLength(0);

		sql.append(makeInsertTrigger(tables));
		sql.append("\n");
		sql.append(makeConstraints(c, tables));
		sql.append("\n");
		
		log.info("SQL:{}", sql);
		s.execute(sql.toString());
		s.close();
		//c.rollback();
	}*/
	
	private String makeInsertTrigger(List<String> tables){
		final StringBuilder sb = new StringBuilder();
		sb.append("CREATE OR REPLACE FUNCTION " + getTriggerName() + "() RETURNS trigger LANGUAGE plpgsql AS $$\n");
		sb.append("BEGIN\n");
		sb.append(getTriggerHeader());
		
		String prev = null;
		int i=0;

		for (String t: tables){
			
			if (prev !=null){				
				sb.append("\t" + (i > 0 ? "ELS":"") + "IF ( " + getTriggerCondition(t) + " )  THEN\n\t\tINSERT INTO " + prev + " VALUES (NEW.*);\n");
				i++;
			}
			prev = t;			
		}
		
		sb.append("\tELSIF ( " + getTriggerLastCondition(prev) + " ) THEN\n\t\tINSERT INTO " + prev + " VALUES (NEW.*);\n");
		
		sb.append("\tELSE\n\t\tRAISE EXCEPTION 'date is out of range';\n\tEND IF;\n\tRETURN NULL;\n");
		sb.append("END; $$;");
		return sb.toString();
	}
	
	private String makeConstraints(Connection c, List<String> tables) throws SQLException{
		final StringBuilder sb = new StringBuilder();
		
		sb.append("CREATE TEMPORARY TABLE " + tableName + "_tmp (LIKE " + getSampleTableName() + ");\n");
		
		for(int i=0; i< tables.size(); i++){

			final String cur = tables.get(i);
			final String prev;
			final String next;
			
			if (i>0)
				prev = tables.get(i-1);
			else
				prev = null;
			
			if (i< tables.size() -1)
				next = tables.get(i+1);
			else
				next = null;
			
			final String check;
			
			if (prev == null){
				check = getFirstConstraint(next);
			}else if (prev !=null && next !=null){
				check = getMiddleConstraint(cur, next);
			}else if (next == null){
				check = getLastConstraint(cur);
			}else{
				throw new RuntimeException("invalid condition");
			}
			
			final String origCheck = getConstraint.queryFirst(c, cur, getConstraintName());
			if (origCheck !=null){
				if (!origCheck.equals(check)){
					sb.append("ALTER TABLE " + cur + " DROP CONSTRAINT " + getConstraintName() + ";\n");
					sb.append("INSERT INTO " + tableName + "_tmp SELECT * FROM " + cur + " WHERE NOT " + check + ";\n");
					sb.append("DELETE FROM " + cur + " WHERE NOT " + check + ";\n");
					sb.append("ALTER TABLE " + cur + " ADD CONSTRAINT " + getConstraintName() + " CHECK " + check + ";\n");
				}else{
					//nothing
				}
			}else{
				sb.append("INSERT INTO " + tableName + "_tmp SELECT * FROM " + cur + " WHERE NOT " + check + ";\n");
				sb.append("DELETE FROM " + cur + " WHERE NOT " + check + ";\n");				
				sb.append("ALTER TABLE " + cur + " ADD CONSTRAINT " + getConstraintName() + " CHECK " + check + ";\n");				
			}
			
			log.debug("orig check: {}", origCheck);
		}
		
		sb.append("INSERT INTO " + tableName + " SELECT * FROM " + tableName + "_tmp;\n");
		
		return sb.toString();
	}
	
	private String makeTable(String sampleTable, String newTable){
		final StringBuilder sb = new StringBuilder();
		sb.append(String.format("CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL) INHERITS (%s);", newTable, sampleTable, tableName));		
		return sb.toString();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		findTables = objectStatements.getQuery(String.class, "select tablename from pg_tables where tablename like ? || '_%'");
		getConstraint = objectStatements.getQuery(String.class, "select consrc from pg_constraint where conrelid = (select oid from pg_class where relname=?) and contype='c' and conname=?");		
	}
}
