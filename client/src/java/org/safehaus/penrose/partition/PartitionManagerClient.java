package org.safehaus.penrose.partition;

import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.partition.PartitionManagerServiceMBean;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionManagerClient extends BaseClient implements PartitionManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(PartitionManagerClient.class);

    public PartitionManagerClient(PenroseClient client) throws Exception {
        super(client, "PartitionManager", getStringObjectName());
    }

    public static String getStringObjectName() {
        return "Penrose:name=PartitionManager";
    }

    public Collection<String> getPartitionNames() throws Exception {
        return (Collection<String>)getAttribute("PartitionNames");
    }

    public void storePartition(String name) throws Exception {
        invoke("storePartition", new Object[] { name }, new String[] { String.class.getName() });
    }

    public void loadPartition(String name) throws Exception {
        invoke("loadPartition", new Object[] { name }, new String[] { String.class.getName() });
    }

    public void unloadPartition(String name) throws Exception {
        invoke("unloadPartition", new Object[] { name }, new String[] { String.class.getName() });
    }

    public void startPartition(String name) throws Exception {
        invoke("startPartition", new Object[] { name }, new String[] { String.class.getName() });
    }

    public void stopPartition(String name) throws Exception {
        invoke("stopPartition", new Object[] { name }, new String[] { String.class.getName() });
    }

    public void startPartitions() throws Exception {
        invoke("startPartitions", new Object[] { }, new String[] { });
    }

    public void stopPartitions() throws Exception {
        invoke("stopPartitions", new Object[] { }, new String[] { });
    }

    public PartitionConfig getPartitionConfig(String partitionName) throws Exception {
        return (PartitionConfig)invoke("getPartitionConfig",
                new Object[] { partitionName },
                new String[] { String.class.getName() }
        );
    }

    public PartitionClient getPartitionClient(String partitionName) throws Exception {
        return new PartitionClient(client, partitionName);
    }

    public void createPartition(PartitionConfig partitionConfig) throws Exception {
        invoke("createPartition",
                new Object[] { partitionConfig },
                new String[] { PartitionConfig.class.getName() }
        );
    }

    public void updatePartition(String name, PartitionConfig partitionConfig) throws Exception {
        invoke("updatePartition",
                new Object[] { name, partitionConfig },
                new String[] { String.class.getName(), PartitionConfig.class.getName() }
        );
    }

    public void removePartition(String name) throws Exception {
        invoke("removePartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showPartitions(PenroseClient client) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();

        System.out.print(TextUtil.rightPad("PARTITION", 15)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 15)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        for (String partitionName : partitionManagerClient.getPartitionNames()) {
            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

            String status;
            try {
                status = partitionClient.getStatus();
            } catch (Exception e) {
                status = "STOPPED";
            }

            System.out.print(TextUtil.rightPad(partitionName, 15) + " ");
            System.out.println(TextUtil.rightPad(status, 10));
        }
    }

    public static void showPartition(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();

        PartitionConfig partitionConfig = partitionManagerClient.getPartitionConfig(partitionName);
        //PartitionClient partitionClient = client.getPartitionClient(partitionName);
        //PartitionConfig partitionConfig = partitionClient.getPartitionConfig();

        System.out.println("Name        : "+partitionConfig.getName());

        String description = partitionConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+partitionConfig.isEnabled());
        //System.out.println("Status      : "+partitionClient.getStatus());
        System.out.println();

        System.out.println("Connections:");
        for (ConnectionConfig connectionConfig : partitionConfig.getConnectionConfigManager().getConnectionConfigs()) {
            System.out.println(" - "+connectionConfig.getName());
        }
        System.out.println();

        System.out.println("Sources:");
        for (SourceConfig sourceConfig : partitionConfig.getSourceConfigManager().getSourceConfigs()) {
            System.out.println(" - "+sourceConfig.getName());
        }
        System.out.println();

        System.out.println("Modules:");
        for (ModuleConfig moduleConfig : partitionConfig.getModuleConfigManager().getModuleConfigs()) {
            System.out.println(" - "+moduleConfig.getName());
        }
    }

    public static void processShowCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("partitions".equals(target)) {
            showPartitions(client);

        } else if ("partition".equals(target)) {
            String partitionName = iterator.next();
            showPartition(client, partitionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void startPartitions(PenroseClient client) throws Exception {

        log.debug("Starting all partitions...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        partitionManagerClient.startPartitions();

        log.debug("All partitions started.");
    }

    public static void startPartition(PenroseClient client, String partitionName) throws Exception {

        log.debug("Starting partition "+partitionName+"...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        partitionClient.start();

        log.debug("Partition "+partitionName+" started.");
    }

    public static void processStartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("partitions".equals(target)) {
            startPartitions(client);

        } else if ("partition".equals(target)) {
            String partitionName = iterator.next();
            startPartition(client, partitionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void stopPartitions(PenroseClient client) throws Exception {

        log.debug("Stopping all partitions...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        partitionManagerClient.stopPartitions();

        log.debug("All partitions stopped.");
    }

    public static void stopPartition(PenroseClient client, String partitionName) throws Exception {

        log.debug("Stopping partition "+partitionName+"...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        partitionClient.stop();

        log.debug("Partition "+partitionName+" stopped.");
    }

    public static void processStopCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("partitions".equals(target)) {
            stopPartitions(client);

        } else if ("partition".equals(target)) {
            String partitionName = iterator.next();
            stopPartition(client, partitionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void restartPartitions(PenroseClient client) throws Exception {
        stopPartitions(client);
        startPartitions(client);
    }

    public static void restartPartition(PenroseClient client, String partitionName) throws Exception {
        stopPartition(client, partitionName);
        startPartition(client, partitionName);
    }

    public static void processRestartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("partitions".equals(target)) {
            restartPartitions(client);

        } else if ("partition".equals(target)) {
            String partitionName = iterator.next();
            restartPartition(client, partitionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String partitionName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();

        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        Object returnValue = partitionClient.invoke(
                methodName,
                paramValues,
                paramTypes
        );

        System.out.println("Return value: "+returnValue);
    }

    public static void processInvokeCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        iterator.next(); // method
        String methodName = iterator.next();
        iterator.next(); // in

        String target = iterator.next();
        if ("partition".equals(target)) {
            String partitionName = iterator.next();

            Object[] paramValues;
            String[] paramTypes;

            if (iterator.hasNext()) {
                iterator.next(); // with

                Collection<Object> values = new ArrayList<Object>();
                Collection<String> types = new ArrayList<String>();

                while (iterator.hasNext()) {
                    String value = iterator.next();
                    values.add(value);
                    types.add(String.class.getName());
                }

                paramValues = values.toArray(new Object[values.size()]);
                paramTypes = types.toArray(new String[types.size()]);

            } else {
                paramValues = new Object[0];
                paramTypes = new String[0];
            }

            invokeMethod(client, partitionName, methodName, paramValues, paramTypes);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void execute(PenroseClient client, Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();
        log.debug("Executing "+command);

        if ("show".equals(command)) {
            processShowCommand(client, iterator);

        } else if ("start".equals(command)) {
            processStartCommand(client, iterator);

        } else if ("stop".equals(command)) {
            processStopCommand(client, iterator);

        } else if ("restart".equals(command)) {
            processRestartCommand(client, iterator);
/*
        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);
*/
        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.partition.PartitionManagerClient [OPTION]... <COMMAND>");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -?, --help         display this help and exit");
        System.out.println("  -P protocol        Penrose JMX protocol");
        System.out.println("  -h host            Penrose server");
        System.out.println("  -p port            Penrose JMX port");
        System.out.println("  -D username        username");
        System.out.println("  -w password        password");
        System.out.println("  -d                 run in debug mode");
        System.out.println("  -v                 run in verbose mode");
        System.out.println();
        System.out.println("Commands:");
        System.out.println();
        System.out.println("  show partitions");
        System.out.println("  show partition <partition name>");
        System.out.println();
        System.out.println("  start partitions");
        System.out.println("  start partition <partition name>");
        System.out.println();
        System.out.println("  stop partitions");
        System.out.println("  stop partition <partition name>");
        System.out.println();
        System.out.println("  restart partitions");
        System.out.println("  restart partition <partition name>");
        System.out.println();
        System.out.println("  invoke method <method name> in partition <partition name> [with <parameter>...]");
    }

    public static void main(String args[]) throws Exception {

        Level level          = Level.WARN;
        String serverType    = PenroseClient.PENROSE;
        String protocol      = PenroseClient.DEFAULT_PROTOCOL;
        String hostname      = "localhost";
        int portNumber       = PenroseClient.DEFAULT_RMI_PORT;
        int rmiTransportPort = PenroseClient.DEFAULT_RMI_TRANSPORT_PORT;

        String bindDn = null;
        String bindPassword = null;

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("PartitionManagerClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection<String> parameters = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    showUsage();
                    System.exit(0);
                    break;
                case 1:
                    parameters.add(getopt.getOptarg());
                    break;
                case 'd':
                    level = Level.DEBUG;
                    break;
                case 'v':
                    level = Level.INFO;
                    break;
                case 'P':
                    protocol = getopt.getOptarg();
                    break;
                case 't':
                    serverType = getopt.getOptarg();
                    break;
                case 'h':
                    hostname = getopt.getOptarg();
                    break;
                case 'p':
                    portNumber = Integer.parseInt(getopt.getOptarg());
                    break;
                case 'r':
                    rmiTransportPort = Integer.parseInt(getopt.getOptarg());
                    break;
                case 'D':
                    bindDn = getopt.getOptarg();
                    break;
                case 'w':
                    bindPassword = getopt.getOptarg();
            }
        }

        if (parameters.size() == 0) {
            showUsage();
            System.exit(0);
        }

        File clientHome = new File(System.getProperty("org.safehaus.penrose.client.home"));

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.safehaus.penrose");

        File log4jXml = new File(clientHome, "conf"+File.separator+"log4j.xml");

        if (level.equals(Level.DEBUG)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (level.equals(Level.INFO)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());

        } else {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);
        }

        try {
            PenroseClient client = new PenroseClient(
                    serverType,
                    protocol,
                    hostname,
                    portNumber,
                    bindDn,
                    bindPassword
            );

            client.setRmiTransportPort(rmiTransportPort);
            client.connect();

            execute(client, parameters);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
