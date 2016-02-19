package com.knockchat.node;

import static com.knockchat.utils.thrift.ThriftProxyFactory.onIfaceAsAsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.thrift.TException;
import org.jgroups.Address;
import org.jgroups.MembershipListener;
import org.jgroups.View;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.Gson;
import com.knockchat.appserver.cluster.MulticastThriftTransport;
import com.knockchat.appserver.cluster.NodeControllersModel;
import com.knockchat.appserver.cluster.NodeListModel;
import com.knockchat.appserver.thrift.cluster.ClusterConfiguration;
import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.appserver.thrift.cluster.Node;
import com.knockchat.appserver.thrift.cluster.NodeControllers;
import com.knockchat.appserver.thrift.cluster.NodeList;
import com.knockchat.appserver.thrift.cluster.VersionnedService;
import com.knockchat.appserver.transport.jgroups.JgroupsMessageDispatcher;
import com.knockchat.utils.GsonSerializer;
import com.knockchat.utils.Pair;

@Component
@ManagedResource(objectName = "bean:name=ScanConfigurationTask")
public class ScanConfigurationTask implements InitializingBean, DisposableBean, MembershipListener, ApplicationListener<ContextStoppedEvent> {

    private static final Logger log = LoggerFactory.getLogger(ScanConfigurationTask.class);
    private static final String STAR = "*";
    private static final String DELIM = ":";

    @Autowired
    private MulticastThriftTransport clusterThriftTransport;

    @Autowired
    private ApplicationContext applicationContext;

    @Resource
    private ThreadPoolTaskScheduler myScheduler;

    private AtomicReference<ClusterConfiguration> conf = new AtomicReference<ClusterConfiguration>();
    private Gson gson = new Gson();

    private volatile boolean isStopped = false;

    public ScanConfigurationTask() {
        log.info("Creating ScanConfigurationTask()");
    }

    @Scheduled(fixedRate = 1000)
    public void scan() {

        if (isStopped)
            return;

        log.debug("scan");
        try {
            final Pair<List<NodeControllersModel>, List<Node>> l = clusterServiceGetConfiguration();

            if (l != null) {
                conf.set(buildClusterConfiguration(l));
            }
        } catch (TException e) {
            log.error("jgroupsMessageDispatcher.getConfiguration() failed", e);
        }
    }

    public Pair<List<NodeControllersModel>, List<Node>> clusterServiceGetConfiguration() throws TException {

        final Map<Address, Node> clusterAnswer = clusterThriftTransport.thriftCall(true, 500, 0, ResponseMode.GET_ALL, onIfaceAsAsync(ClusterService.Iface.class).getNodeConfiguration());

        final List<NodeControllersModel> ret = new ArrayList<NodeControllersModel>();
        final List<Node> nodes = Lists.newArrayList();

        for (Entry<Address, Node> it : clusterAnswer.entrySet()) {
            for (NodeControllers c : it.getValue().getControllers()) {
                ret.add(new NodeControllersModel(c, it.getKey()));
            }

            nodes.add(new Node(null, it.getValue().getName(), it.getValue().getJmxmp(), it.getValue().getHttp()));
        }

        return new Pair<List<NodeControllersModel>, List<Node>>(ret, nodes);
    }

    @PreDestroy
    public void preDestroy() {
        myScheduler.setWaitForTasksToCompleteOnShutdown(true);
        myScheduler.shutdown();
    }

    private ClusterConfiguration buildClusterConfiguration(Pair<List<NodeControllersModel>, List<Node>> l) {
        final ClusterConfiguration ret = new ClusterConfiguration(new HashMap<String, VersionnedService>(), l.second);

        for (NodeControllersModel c : l.first) {
            for (String name : c.getExternalControllers()) {

                VersionnedService v = ret.getServices().get(name);

                if (v == null) {
                    v = new VersionnedService(new HashMap<String, NodeList>());
                    ret.getServices().put(name, v);
                }

                NodeListModel n = (NodeListModel) v.getVersions().get(c.getVersion());
                if (n == null) {
                    n = new NodeListModel(new ArrayList<Address>(), new ArrayList<String>(), new ArrayList<Integer>(), 0);
                    v.getVersions().put(c.getVersion(), n);
                }
                n.jGroupsAddresses.add(c.jGroupsAddress);
                n.getHosts().add(c.getAddress().getHost());
                n.getPorts().add(c.getAddress().getPort());
            }

        }

        for (VersionnedService vs : ret.getServices().values()) {
            for (NodeList nl : vs.getVersions().values()) {
                final List<Object> ho = new ArrayList<Object>();
                ho.addAll(nl.getHosts());
                ho.addAll(nl.getPorts());
                nl.setHash(Arrays.hashCode(ho.toArray()));
            }
        }
        return compactConfiguration(ret);
    }

    private ClusterConfiguration compactConfiguration(ClusterConfiguration before) {
        Map<String, VersionnedService> result = new HashMap<>();
        HashMap<String, TreeMultimap<VersionnedService, String>> serviceNameToMapVersionToMethod = Maps.newHashMap(); //Service -> MultiMap<VersionnedService, Method >
        Multimap<VersionnedService, Pair<String, Integer>> defaultRoute4Service = HashMultimap.create(); // VersionnedService -> (Service:* , Weight)

        for (Entry<String, VersionnedService> entry : before.getServices().entrySet()) {
            String[] serviceMethod = entry.getKey().split(":");
            if (!serviceNameToMapVersionToMethod.containsKey(serviceMethod[0])) {
                serviceNameToMapVersionToMethod.put(serviceMethod[0], TreeMultimap.<VersionnedService, String>create());
            }
            serviceNameToMapVersionToMethod.get(serviceMethod[0]).put(entry.getValue(), serviceMethod[1]);
        }

        for (Entry<String, TreeMultimap<VersionnedService, String>> serviceEntry : serviceNameToMapVersionToMethod.entrySet()) {
            Entry<VersionnedService, Collection<String>> methodMaxEntry = Collections.max(serviceEntry.getValue().asMap().entrySet()
                    , new Comparator<Entry<VersionnedService, Collection<String>>>() {
                @Override
                public int compare(Entry<VersionnedService, Collection<String>> o1, Entry<VersionnedService, Collection<String>> o2) {
                    return Integer.compare(o1.getValue().size(), o2.getValue().size());
                }
            });
            defaultRoute4Service.put(methodMaxEntry.getKey(), new Pair<String, Integer>(serviceEntry.getKey(), methodMaxEntry.getValue().size()));  //Находим max(Service:*)
        }

        Entry<VersionnedService,Collection<Pair<String, Integer>>> defaultRoute = Collections.max(defaultRoute4Service.asMap().entrySet(), new Comparator<Entry<VersionnedService, Collection<Pair<String, Integer>>>>() {
            @Override
            public int compare(Entry<VersionnedService, Collection<Pair<String, Integer>>> o1, Entry<VersionnedService, Collection<Pair<String, Integer>>> o2) {
                int sizeo1=0, sizeo2=0;
                for (Pair<String, Integer> pair :o1.getValue())
                    sizeo1+=pair.second;
                for (Pair<String, Integer> pair :o2.getValue())
                    sizeo2+=pair.second;
                return Integer.compare(sizeo1, sizeo2);
            }
        });

        result.put("*",defaultRoute.getKey()); // *
        for (Entry<VersionnedService, Pair<String, Integer>> serviceEntry : defaultRoute4Service.entries()){
            if (!serviceEntry.getKey().equals(defaultRoute.getKey()))
                result.put(serviceEntry.getValue().first.concat(":*"),serviceEntry.getKey());           //service:*
        }
        for (Entry<String, TreeMultimap<VersionnedService, String>> serviceEntry : serviceNameToMapVersionToMethod.entrySet()) {
            for (Entry<VersionnedService, Collection<String>> methodEntry : serviceEntry.getValue().asMap().entrySet()){
                if (!defaultRoute4Service.containsKey(methodEntry.getKey())){
                    String keyPrefix = serviceEntry.getKey().concat(":");
                    for (String methodName :  methodEntry.getValue())
                        result.put(keyPrefix.concat(methodName),methodEntry.getKey());
                }
            }
        }
        return new ClusterConfiguration(result, before.getNodes());
    }

    public ClusterConfiguration getConfiguration() {
        ClusterConfiguration l = conf.get();
        if (l != null)
            return l;

        try {
            l = buildClusterConfiguration(clusterServiceGetConfiguration());
        } catch (TException e) {
            log.error("jgroupsMessageDispatcher.getConfiguration() failed", e);
        }

        return l;
    }

    public String getConfigurationJSON() {
        final ClusterConfiguration configuration = getConfiguration();
        return configuration == null ? null : GsonSerializer.toJson(configuration);
    }

    @ManagedOperation(description = "log cluster configuration")
    public String logClusterConfiguration() {
        final String ret = getConfiguration().toString();
        log.info("{}", ret);
        return ret;
    }

    public void reset() {
        conf.set(null);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    	if (clusterThriftTransport instanceof JgroupsMessageDispatcher)
    		((JgroupsMessageDispatcher)clusterThriftTransport).addMembershipListener(this);
    }

    @Override
    public void destroy() throws Exception {
    	if (clusterThriftTransport instanceof JgroupsMessageDispatcher)
    		((JgroupsMessageDispatcher)clusterThriftTransport).removeMembershipListener(this);
    }

    @Override
    public void viewAccepted(View new_view) {
        reset();
    }

    @Override
    public void suspect(Address suspected_mbr) {
        reset();
    }

    @Override
    public void block() {
        // TODO Auto-generated method stub

    }

    @Override
    public void unblock() {
        // TODO Auto-generated method stub

    }

    @Override
    public void onApplicationEvent(ContextStoppedEvent arg0) {
        isStopped = true;
    }
}
