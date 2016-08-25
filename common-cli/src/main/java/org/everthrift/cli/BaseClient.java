package org.everthrift.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.everthrift.cli.env.ClusterModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseClient {

    private final Map<String, BaseModule> availableModules = new HashMap<>();

    public final Logger LOG = LoggerFactory.getLogger(BaseClient.class);

    protected final Options moduleOptions = new Options();

    protected final Options moduleOptions4Help = new Options();

    protected final Options argumentOptions4Help = new Options();

    protected final OptionGroup argumentGroup = new OptionGroup();

    protected final OptionGroup moduleGroup = new OptionGroup();

    protected final OptionGroup configGroup = new OptionGroup();

    /**
     * инициализируем модули которые будут видны клиенту
     */
    protected void initModules() {
        addModule(new ClusterModule());
    }

    protected void addModule(BaseModule module) {
        availableModules.put(module.getModule().getOpt(), module);
    }

    protected Map<String, BaseModule> getModules() {
        return availableModules;
    }

    protected void init() {
        initModules();

        moduleGroup.setRequired(true);
        argumentGroup.setRequired(true);
        configGroup.setRequired(true);

        for (BaseModule module : getModules().values()) {

            // добавление новых модулей (пока только cache и cluster)
            moduleGroup.addOption(module.getModule());

            // аргументы для модулей (взаимоисключаемые)
            for (Option option : (List<Option>) module.getOptions()) {

                boolean isReused = false;
                for (Option old : (Collection<Option>) argumentGroup.getOptions()) {
                    if (old.getOpt().equals(option.getOpt())) {
                        old.setDescription(old.getDescription() + "\n" + option.getDescription());
                        isReused = true;
                        break;
                    }
                }

                if (!isReused)
                    argumentGroup.addOption(option);
            }

        }

        OptionBuilder.withLongOpt("infonode");
        OptionBuilder.isRequired();
        OptionBuilder.hasArgs(2);
        OptionBuilder.withArgName("host port");
        OptionBuilder.withDescription("infonode <host> <port>");
        final Option infonode = OptionBuilder.create("i");
        OptionBuilder.withLongOpt("file");
        OptionBuilder.isRequired();
        OptionBuilder.hasArg();
        OptionBuilder.withArgName("conf.json");
        OptionBuilder.withDescription("cluster config location");
        final Option conf = OptionBuilder.create("f");
        configGroup.addOption(infonode);
        configGroup.addOption(conf);

        moduleOptions.addOptionGroup(moduleGroup);
        moduleOptions4Help.addOptionGroup(moduleGroup);

        moduleOptions.addOptionGroup(argumentGroup);
        argumentOptions4Help.addOptionGroup(argumentGroup);

        moduleOptions.addOptionGroup(configGroup);
        argumentOptions4Help.addOptionGroup(configGroup);

    }

    protected void run(String[] args) {
        BasicParser parser = new BasicParser();
        PrintWriter pw = new PrintWriter(System.out);
        try {
            CommandLine moduleLine = parser.parse(moduleOptions, args, false);

            switch (configGroup.getSelected()) {
            case "f":
                if (moduleLine.getOptionValue("file") == null)
                    throw new IllegalArgumentException("provide config file location");
                getModule(moduleGroup.getSelected(), moduleLine.getOptionValue("file")).runModule(pw, argumentGroup.getSelected(),
                                                                                                  moduleLine);
                break;
            case "i":
                String[] conf_args = moduleLine.getOptionValues("infonode");
                if (conf_args.length != 2)
                    throw new ParseException("invalid parameters for infonode connection");
                getModule(moduleGroup.getSelected(), conf_args[0], Integer.parseInt(conf_args[1])).runModule(pw,
                                                                                                             argumentGroup.getSelected(),
                                                                                                             moduleLine);
                break;
            default:
                throw new ParseException("Set infonode host:port or cluster configuration in json");
            }
        }
        catch (ParseException e) {
            LOG.error("Argument Error: {}", e);
            printHelp();
        }
        catch (NumberFormatException nfe) {
            LOG.error("Argument Format Error: {}", nfe);
        }
        catch (TException te) {
            LOG.error("Thrift Error: {}", te);
        }
        catch (Exception e) {
            LOG.error(" Error: {}", e);
        }
        finally {
            pw.close();
        }
    }

    private BaseModule getModule(String key, String host, int port) {
        BaseModule module = availableModules.get(key);
        module.init(host, port);
        return module;
    }

    private BaseModule getModule(String key, String configLocation) throws IOException {
        BaseModule module = availableModules.get(key);
        StringBuilder config = new StringBuilder();
        for (String line : Files.readAllLines(Paths.get(configLocation), Charset.defaultCharset()))
            config.append(line);
        module.init(config.toString());
        return module;
    }

    public static void main(String[] args) {
        BaseClient client = new BaseClient();
        client.init();
        client.run(args);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>() {
            @Override
            public int compare(Option o1, Option o2) {
                if (o1.getType() != null && o2.getType() != null) {
                    return String.CASE_INSENSITIVE_ORDER.compare(o1.getType().toString(), o2.getType().toString());
                }
                return 0;
            }
        });
        PrintWriter pw = new PrintWriter(System.out);
        formatter.printHelp(pw, 100, "cli <module> <method> (-i <host port> | -f <config.json>)", null, moduleOptions4Help, 2, 3, null);
        pw.println("args:");
        formatter.printOptions(pw, 100, argumentOptions4Help, 2, 3);
        pw.close();
    }

}
