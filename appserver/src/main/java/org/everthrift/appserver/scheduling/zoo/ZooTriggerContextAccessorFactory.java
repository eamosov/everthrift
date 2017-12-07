package org.everthrift.appserver.scheduling.zoo;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.everthrift.appserver.scheduling.DuplicatedTaskException;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessor;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessorFactory;
import org.everthrift.utils.ClassUtils;
import org.everthrift.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.everthrift.utils.ExceptionUtils.asUnchecked;

/**
 * Created by fluder on 06/09/17.
 */
public class ZooTriggerContextAccessorFactory implements TriggerContextAccessorFactory {

    @Autowired
    CuratorFramework curator;

    final Gson gson = new GsonBuilder().setDateFormat(DateFormat.FULL, DateFormat.FULL).setPrettyPrinting().create();

    @Value("${distributed_scheduler.zoo.prefix:}")
    String prefix;

    @NotNull
    @Override
    public TriggerContextAccessor get(String taskName, boolean isDynamic) {
        return new ZooTriggerContextAccessorImpl(this, taskName, isDynamic);
    }

    @NotNull
    String path(String taskName) {
        return prefix + "/scheduler/" + taskName;
    }

    @NotNull
    @Override
    public TriggerContextAccessor createDynamic(String taskName, long period, long lastScheduledExecutionTime, String beanName, @Nullable Serializable arg) throws DuplicatedTaskException {

        final ZooData zooData = new ZooData();

        zooData.lastCompletionTime = new Date();
        zooData.lastScheduledExecutionTime = new Date(lastScheduledExecutionTime);
        zooData.beanName = beanName;
        zooData.period = period;
        zooData.dynamic = true;
        zooData.cancelled = false;
        if (arg != null) {
            zooData.arg = ClassUtils.writeObject(arg);
        }

        try {
            curator.create().creatingParentsIfNeeded().forPath(path(taskName), gson.toJson(zooData).getBytes());
        } catch (KeeperException.NodeExistsException e) {
            throw new DuplicatedTaskException(taskName);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return new ZooTriggerContextAccessorImpl(this, taskName, true);
    }

    @Override
    public List<String> getAllDynamic() {

        try {
            return curator.getChildren()
                          .forPath(prefix + "/scheduler")
                          .stream()
                          .map(taskName -> Pair.create(taskName,
                                                       gson.fromJson(new String(asUnchecked(() -> curator.getData()
                                                                                                         .forPath(path(taskName)))),
                                                                     ZooData.class)))
                          .filter(p -> p.second.isDynamic() && !p.second.cancelled)
                          .map(p -> p.first)
                          .collect(Collectors.toList());
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
