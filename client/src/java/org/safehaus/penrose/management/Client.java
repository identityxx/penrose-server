package org.safehaus.penrose.management;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.service.ServiceClient;
import org.safehaus.penrose.service.ServiceManagerClient;

import java.util.*;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

public class Client {

    public static Logger log = Logger.getLogger(Client.class);

    private PenroseClient client;

    public Client(
            String serverType,
            String protocol,
            String hostname,
            int port,
            String bindDn,
            String bindPassword
    ) throws Exception {

        client = new PenroseClient(
                serverType,
                protocol,
                hostname,
                port,
                bindDn,
                bindPassword
        );
    }

    public void setRmiTransportPort(int rmiTransportPort) {
        client.setRmiTransportPort(rmiTransportPort);
    }

    public void connect() throws Exception {
        client.connect();
    }

    public void close() throws Exception {
        client.close();
    }

    public void showServices() throws Exception {
        System.out.print(TextUtil.rightPad("SERVICE", 15)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 15)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        
        for (String serviceName : serviceManagerClient.getServiceNames()) {
            ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
            String status = serviceClient.getStatus();

            System.out.print(TextUtil.rightPad(serviceName, 15) + " ");
            System.out.println(TextUtil.rightPad(status, 10));
        }
    }

    public void showService(String serviceName) throws Exception {
        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
        ServiceConfig serviceConfig = serviceClient.getServiceConfig();

        System.out.println("Name        : "+serviceConfig.getName());
        System.out.println("Class       : "+serviceConfig.getServiceClass());

        String description = serviceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Enabled     : "+serviceConfig.isEnabled());
        System.out.println("Status      : "+serviceClient.getStatus());
        System.out.println();

        System.out.println("Parameters: ");
        for (String paramName : serviceConfig.getParameterNames()) {
            String value = serviceConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
    }

    public void showPartitions() throws Exception {
        System.out.print(TextUtil.rightPad("PARTITION", 15)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 15)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        for (String partitionName : partitionManagerClient.getPartitionNames()) {
            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            String status = partitionClient.getStatus();

            System.out.print(TextUtil.rightPad(partitionName, 15) + " ");
            System.out.println(TextUtil.rightPad(status, 10));
        }
    }

    public void showPartition(String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionConfig partitionConfig = partitionManagerClient.getPartitionConfig(partitionName);
        //PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
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

    public void processShowCommand(Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("services".equals(target)) {
            showServices();

        } else if ("service".equals(target)) {
            String serviceName = iterator.next();
            showService(serviceName);

        } else if ("partitions".equals(target)) {
            showPartitions();

        } else if ("partition".equals(target)) {
            String partitionName = iterator.next();
            showPartition(partitionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processStartCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
            ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
            serviceClient.start();

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            partitionClient.start();

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processStopCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
            ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
            serviceClient.stop();

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            partitionClient.stop();

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processRestartCommand(Iterator iterator) throws Exception {

        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
            ServiceClient serviceClient = serviceManagerClient.getServiceClient(serviceName);
            serviceClient.stop();
            serviceClient.start();

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            partitionClient.stop();
            partitionClient.start();

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void execute(Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();
        log.debug("Executing "+command);

        if ("version".equals(command)) {
            String version = client.getProductName()+" "+client.getProductVersion();
            System.out.println(version);

        } else if ("show".equals(command)) {
            processShowCommand(iterator);

        } else if ("start".equals(command)) {
            processStartCommand(iterator);

        } else if ("stop".equals(command)) {
            processStopCommand(iterator);

        } else if ("restart".equals(command)) {
            processRestartCommand(iterator);

        } else if ("reload".equals(command)) {
            client.reload();

        } else if ("store".equals(command)) {
            client.store();

        } else if ("rename".equals(command)) {
            String object = iterator.next();
            if ("entry".equals(object)) {
                String oldDn = iterator.next();
                String newDn = iterator.next();
                log.debug("Renaming "+oldDn+" to "+newDn);
                client.renameEntryConfig(oldDn, newDn);
            }

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.management.Client [OPTION]... <COMMAND>");
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
        System.out.println("  version");
        System.out.println();
        System.out.println("  show services");
        System.out.println("  show service <service name>");
        System.out.println("  start service <service name>");
        System.out.println("  stop service <service name>");
        System.out.println("  restart service <service name>");
        System.out.println();
        System.out.println("  show partitions");
        System.out.println("  show partition <partition name>");
        System.out.println("  start partition <partition name>");
        System.out.println("  stop partition <partition name>");
        System.out.println("  restart partition <partition name>");
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

        Getopt getopt = new Getopt("Client", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection<String> parameters = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    Client.showUsage();
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

        File serviceHome = new File(System.getProperty("org.safehaus.penrose.management.home"));
        
        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");

        File log4jXml = new File(serviceHome, "conf"+File.separator+"log4j.xml");

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
            Client client = new Client(
                    serverType,
                    protocol,
                    hostname,
                    portNumber,
                    bindDn,
                    bindPassword
            );

            client.setRmiTransportPort(rmiTransportPort);
            client.connect();

            client.execute(parameters);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }
}
