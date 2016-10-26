package org.everthrift.cassandra.scheduling.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.everthrift.cassandra.scheduling.DuplicatedTaskException;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessor;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessorFactory;
import org.everthrift.utils.ClassUtils;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

public class CasTriggerContextAccessorFactory implements TriggerContextAccessorFactory {

    private Session session;

    private String tableName = "distributed_scheduler";

    private boolean createTable = true;

    @PostConstruct
    public void initialize() {
        if (createTable) {
            session.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (id text PRIMARY KEY, last_actual_execution_time timestamp, last_completion_time timestamp, last_scheduled_execution_time timestamp, period bigint, bean_name text, arg blob, cancelled boolean, dynamic boolean, serial bigint)");
        }
    }

    @Override
    public TriggerContextAccessor get(String taskName, boolean isDynamic) {
        return new TriggerContextAccessorImpl(this, taskName, isDynamic);
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

    @Override
    public TriggerContextAccessor createDynamic(String taskName, long period, long lastScheduledExecutionTime, String beanName, Serializable arg) throws DuplicatedTaskException {

        final ResultSet rs = session.execute(insertInto(tableName).value("id", taskName)
                                                                  .value("serial", 0)
                                                                  .value("last_completion_time", new Date())
                                                                  .value("last_scheduled_execution_time", lastScheduledExecutionTime)
                                                                  .value("bean_name", beanName)
                                                                  .value("period", period)
                                                                  .value("dynamic", true)
                                                                  .value("cancelled", false)
                                                                  .value("arg", arg != null ? ByteBuffer.wrap(ClassUtils.writeObject(arg)) : null)
                                                                  .ifNotExists());
        if (!rs.wasApplied()) {
            throw new DuplicatedTaskException(taskName);
        }

        return new TriggerContextAccessorImpl(this, taskName, true);
    }

    @Override
    public List<String> getAllDynamic() {
        return session.execute(QueryBuilder.select("id")
                                           .from(tableName)
                                           .allowFiltering()
                                           .where(eq("dynamic", true))
                                           .and(eq("cancelled", false)))
                      .all()
                      .stream()
                      .map(rs -> rs.getString("id"))
                      .collect(Collectors.toList());
    }
}
