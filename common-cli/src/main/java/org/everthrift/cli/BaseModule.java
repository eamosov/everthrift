package org.everthrift.cli;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

public abstract class BaseModule<T extends TServiceClient> {

    public static final Comparator VERSION_COMPARATOR = new VersionComparator();

    protected String infonodeHost;
    protected int infonodePort;
    protected String jsonConfig;

    public void init(String host, int port) {
        this.infonodeHost = host;
        this.infonodePort = port;
    }

    public void init(String clusterConfJson){
        this.jsonConfig = clusterConfJson;
    }

    private String lastVersion(Collection<String> versions){
        return  Collections.<String>max(versions, VERSION_COMPARATOR);
    }


    public T getClient(String host, int port) throws TTransportException {
        return newClient(host, port);
    }

    protected abstract T newClient(String host, int port) throws TTransportException;

    public abstract Option getModule();

    public abstract List<Option> getOptions();

    public abstract void runModule(PrintWriter out, String option, CommandLine cmd) throws Exception;

    public static class VersionComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            Iterator<Integer> o1iter = splitVersion(o1).iterator();
            Iterator<Integer> o2iter = splitVersion(o2).iterator();
            int result = 0;
            while ((o1iter.hasNext() && o2iter.hasNext()) && result != 0) {
                result = Integer.compare(o1iter.next(), o2iter.next());
            }
            if (result != 0) return result;
            else if (o1iter.hasNext()) return 1;
            else if (o2iter.hasNext()) return -1;
            return result;
        }

        public List<Integer> splitVersion(String version) {
            List<Integer> res = new ArrayList<>();
            String numberWithDots = version.replaceAll("[^\\d\\.]", "");
            for (String var : numberWithDots.split(".")) {
                res.add(Integer.parseInt(var));
            }
            return res;
        }


    }

}
