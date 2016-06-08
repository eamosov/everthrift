package com.knockchat.cli.env;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.cli.BaseModule;
import com.knockchat.cli.ThriftUtills;

public class ClusterModule extends BaseModule<ClusterService.Client> {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterModule.class);
    public static final String SERVICE_NAME = "ClusterService";
    public static final String GET_CLUSTER_CONFIGURATION_JSON = "getClusterConfigurationJSON";
    private static final Option module = OptionBuilder.withLongOpt("cluster").withDescription("run cluster service client").withType("cluster").create("c");
    private static final Option[] options = new Option[]{OptionBuilder.withDescription("cluster service: get cluster config(json)").withType("cluster").withLongOpt("cluster-conf").create("gc")};


    @Override
    protected ClusterService.Client newClient(String host, int port) throws TTransportException {
        return new ClusterService.Client(ThriftUtills.getProtocol(host, port));
    }

    @Override
    public Option getModule() {
        return module;
    }

    @Override
    public List<Option> getOptions() {
        return Arrays.asList(options);
    }




    @Override
	public void runModule(PrintWriter out, String option, CommandLine cmd) throws Exception {
        switch (option) {
            case "gc": {
                out.println("\n\n");
                out.print("not implemented");
                out.println("\n\n");
                break;
            }
            default: {
                throw new RuntimeException("Please insert option for cluster module (gc  for get Config)");
            }
        }
    }
}
