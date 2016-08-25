package org.everthrift.clustering.jgroups;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.everthrift.services.thrift.cluster.Node;
import org.jgroups.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class NodeDb {
    private static final Logger log = LoggerFactory.getLogger(NodeDb.class);

    private long failedTimeout = 5 * 1000;

    private final Random rnd = new Random();

    private static class NodeDesc {
        final Address address;

        final String sAddress;

        Node node;

        long failedAt;

        long successedAt;

        NodeDesc(Address address, Node node) {
            super();
            this.address = address;
            this.sAddress = address.toString();
            setNode(node);
        }

        void setNode(Node node) {
            this.node = node != null ? new Node(node) : null;
        }

        Address getAddress() {
            return address;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((sAddress == null) ? 0 : sAddress.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            NodeDesc other = (NodeDesc) obj;
            if (sAddress == null) {
                if (other.sAddress != null)
                    return false;
            } else if (!sAddress.equals(other.sAddress))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "NodeDesc [address=" + address + ", node=" + node + ", failedAt=" + failedAt + ", successedAt=" + successedAt + "]";
        }

    }

    private Multimap<String, NodeDesc> methodsMap = ArrayListMultimap.create(); // Method
                                                                                // name
                                                                                // ->
                                                                                // List<NodeDescr>

    private Map<String, NodeDesc> addresses = Maps.newHashMap(); // address ->
                                                                 // NodeDescr

    private void _removeNode(String _a) {
        final NodeDesc d = addresses.remove(_a);
        if (d != null && d.node.isSetControllers()) {
            d.node.getControllers().forEach(m -> methodsMap.remove(m, d));
        }
    }

    public synchronized void removeNode(Address a) {
        _removeNode(a.toString());
    }

    public synchronized void addNode(Address a, Node n) {
        final String _a = a.toString();
        NodeDesc d = addresses.get(_a);

        if (d != null) {
            if (n.equals(d.node))
                return;

            if (d.node.isSetControllers()) {
                for (String m : d.node.getControllers())
                    methodsMap.remove(m, d);
            }

            d.setNode(n);
        } else {
            d = new NodeDesc(a, n);
        }

        addresses.put(_a, d);
        if (d.node.isSetControllers()) {
            for (String m : d.node.getControllers())
                methodsMap.put(m, d);
        }
    }

    public synchronized List<Address> getNode(String methodName) {
        final long now = System.currentTimeMillis();

        @SuppressWarnings({ "rawtypes", "unchecked" })
        final List<NodeDesc> nodes = (List) methodsMap.get(methodName);
        if (nodes == null)
            return Collections.emptyList();

        final List<Address> aa = Lists.newArrayList(Iterables.transform(Iterables.filter(nodes, n -> (n.failedAt < now - failedTimeout)),
                                                                        NodeDesc::getAddress));
        Collections.shuffle(aa, rnd);
        return aa;
    }

    public synchronized void failed(Address a) {
        final NodeDesc d = addresses.get(a.toString());
        if (d != null) {
            d.failedAt = System.currentTimeMillis();
        }
    }

    public synchronized void success(Address a) {
        final NodeDesc d = addresses.get(a.toString());
        if (d != null) {
            d.successedAt = System.currentTimeMillis();
        }
    }

    public synchronized void retain(List<Address> aa) {
        final Set<String> _aa = ImmutableSet.copyOf(Lists.transform(aa, Address::toString));
        for (String a : ImmutableList.copyOf(addresses.keySet())) {
            if (!_aa.contains(a)) {
                log.debug("remove {}", a);
                _removeNode(a);
            }
        }
    }

    @Override
    public String toString() {
        return "NodeDb [methodsMap=" + methodsMap + "]";
    }

}
