package org.everthrift.cassandra.scheduling.cassandra;

import com.datastax.driver.core.Session;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessor;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessorFactory;

import javax.annotation.PostConstruct;

public class CasTriggerContextAccessorFactory implements TriggerContextAccessorFactory {

    private Session session;

    private String tableName = "distributed_scheduler";

    private boolean createTable = true;

    @PostConstruct
    public void initialize() {
        if (createTable) {
            session.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (id text PRIMARY KEY, last_actual_execution_time timestamp, last_completion_time timestamp, last_scheduled_execution_time timestamp, serial bigint)");
        }
    }

    @Override
    public TriggerContextAccessor get(String taskName) {
        return new TriggerContextAccessorImpl(this, taskName);
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public CasTriggerContextAccessorFactory() {

    }

    public CasTriggerContextAccessorFactory(Session session) {
        this.session = session;
    }

    public boolean isCreateTable() {
        return createTable;
    }

    public void setCreateTable(boolean createTable) {
        this.createTable = createTable;
    }
}
