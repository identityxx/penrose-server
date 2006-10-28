package org.safehaus.penrose.client;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.connection.ConnectionClient;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionCounter;
import org.safehaus.penrose.connection.ConnectionManagerClient;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.service.ServiceClient;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.service.ServiceManagerClient;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.module.ModuleManagerClient;
import org.safehaus.penrose.source.SourceManagerClient;
import org.safehaus.penrose.source.SourceClient;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.SourceCounter;

import java.util.*;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

public class Client {

    public static Logger log = Logger.getLogger(Client.class);

    PenroseClient client;

    public Client(
            String serverType,
            String protocol,
            String hostname,
            int port,
            String username,
            String password
    ) throws Exception {

        client = new PenroseClient(
                serverType,
                protocol,
                hostname,
                port,
                username,
                password
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

    public void printServices() throws Exception {
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        ServiceManagerClient serviceManagerClient = client.getServiceManagerClient();
        for (Iterator i=serviceManagerClient.getServiceNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String status = serviceManagerClient.getStatus(name);

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(name, 15)+" ");
            System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
        }
    }

    public void printPartitions() throws Exception {
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        for (Iterator i=partitionManagerClient.getPartitionNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String status = partitionManagerClient.getStatus(name);

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(name, 15)+" ");
            System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
        }
    }

    public void printConnections() throws Exception {

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("CONNECTION", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        ConnectionManagerClient connectionManagerClient = client.getConnectionManagerClient();
        for (Iterator i=connectionManagerClient.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=connectionManagerClient.getConnectionNames(partitionName).iterator(); j.hasNext(); ) {
                String connectionName = (String)j.next();
                String status = connectionManagerClient.getStatus(partitionName, connectionName);

                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(connectionName, 15)+" ");
                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(partitionName, 15)+" ");
                System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
            }
        }
    }

    public void printSources() throws Exception {

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("SOURCE", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        SourceManagerClient sourceManagerClient = client.getSourceManagerClient();
        for (Iterator i=sourceManagerClient.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=sourceManagerClient.getSourceNames(partitionName).iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                String status = sourceManagerClient.getStatus(partitionName, sourceName);

                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(sourceName, 15)+" ");
                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(partitionName, 15)+" ");
                System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
            }
        }
    }

    public void printModules() throws Exception {

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("MODULE", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.rightPad("STATUS", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 15)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        ModuleManagerClient moduleManagerClient = client.getModuleManagerClient();
        for (Iterator i=moduleManagerClient.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=moduleManagerClient.getModuleNames(partitionName).iterator(); j.hasNext(); ) {
                String moduleName = (String)j.next();
                String status = moduleManagerClient.getStatus(partitionName, moduleName);

                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(moduleName, 15)+" ");
                System.out.print(org.safehaus.penrose.util.Formatter.rightPad(partitionName, 15)+" ");
                System.out.println(org.safehaus.penrose.util.Formatter.rightPad(status, 10));
            }
        }
    }

    public void printService(String serviceName) throws Exception {
        ServiceClient serviceClient = client.getServiceManagerClient().getService(serviceName);
        ServiceConfig serviceConfig = serviceClient.getServiceConfig();

        System.out.println("Service     : "+serviceConfig.getName());
        System.out.println("Class       : "+serviceConfig.getServiceClass());

        String description = serviceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Status      : "+serviceClient.getStatus());
        System.out.println();

        System.out.println("Parameters  : ");
        for (Iterator i=serviceConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String paramName = (String)i.next();
            String value = serviceConfig.getParameter(paramName);
            System.out.println(" - "+paramName +": "+value);
        }
    }

    public void printPartition(String partitionName) throws Exception {
        PartitionClient partitionClient = client.getPartitionManagerClient().getPartitionClient(partitionName);
        PartitionConfig partitionConfig = partitionClient.getPartitionConfig();

        System.out.println("Partition   : "+partitionConfig.getName());
        System.out.println("Path        : "+partitionConfig.getPath());
        System.out.println("Enabled     : "+partitionConfig.isEnabled());
        System.out.println("Status      : "+partitionClient.getStatus());
    }

    public void printConnection(String partitionName, String connectionName) throws Exception {
        ConnectionClient connectionClient = client.getConnectionManagerClient().getConnectionClient(partitionName, connectionName);
        ConnectionConfig connectionConfig = connectionClient.getConnectionConfig();

        System.out.println("Connection  : "+connectionConfig.getName());
        System.out.println("Partition   : "+partitionName);
        System.out.println("Adapter     : "+connectionConfig.getAdapterName());

        String description = connectionConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Status      : "+connectionClient.getStatus());
        System.out.println();

        System.out.println("Parameters  :");
        for (Iterator i=connectionConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String paramName = (String)i.next();
            String value = connectionConfig.getParameter(paramName);
            System.out.println(" - "+paramName +": "+value);
        }
        System.out.println();

        ConnectionCounter counter = connectionClient.getCounter();
        System.out.println("Counters    :");
        System.out.println(" - add      : "+counter.getAddCounter());
        System.out.println(" - bind     : "+counter.getBindCounter());
        System.out.println(" - delete   : "+counter.getDeleteCounter());
        System.out.println(" - load     : "+counter.getLoadCounter());
        System.out.println(" - modify   : "+counter.getModifyCounter());
        System.out.println(" - search   : "+counter.getSearchCounter());
    }

    public void printSource(String partitionName, String sourceName) throws Exception {
        SourceClient sourceClient = client.getSourceManagerClient().getSourceClient(partitionName, sourceName);
        SourceConfig sourceConfig = sourceClient.getSourceConfig();

        System.out.println("Source      : "+sourceConfig.getName());
        System.out.println("Partition   : "+partitionName);

        String description = sourceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Status      : "+sourceClient.getStatus());
        System.out.println();

        System.out.println("Parameters  :");
        for (Iterator i=sourceConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String paramName = (String)i.next();
            String value = sourceConfig.getParameter(paramName);
            System.out.println(" - "+paramName +": "+value);
        }
        System.out.println();

        SourceCounter counter = sourceClient.getCounter();
        System.out.println("Counters    :");
        System.out.println(" - add      : "+counter.getAddCounter());
        System.out.println(" - bind     : "+counter.getBindCounter());
        System.out.println(" - delete   : "+counter.getDeleteCounter());
        System.out.println(" - modify   : "+counter.getModifyCounter());
        System.out.println(" - modrdn   : "+counter.getModRdnCounter());
        System.out.println(" - search   : "+counter.getSearchCounter());
    }

    public void printModule(String partitionName, String moduleName) throws Exception {
        ModuleClient moduleClient = client.getModuleManagerClient().getModuleClient(partitionName, moduleName);
        ModuleConfig moduleConfig = moduleClient.getModuleConfig();

        System.out.println("Connection  : "+moduleConfig.getName());
        System.out.println("Partition   : "+partitionName);

        String description = moduleConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Status      : "+moduleClient.getStatus());
        System.out.println();

        System.out.println("Parameters  :");
        for (Iterator i=moduleConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String paramName = (String)i.next();
            String value = moduleConfig.getParameter(paramName);
            System.out.println(" - "+paramName +": "+value);
        }
        System.out.println();
    }

    public void processShowCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("services".equals(target)) {
            printServices();

        } else if ("partitions".equals(target)) {
            printPartitions();

        } else if ("connections".equals(target)) {
            printConnections();

        } else if ("sources".equals(target)) {
            printSources();

        } else if ("modules".equals(target)) {
            printModules();

        } else if ("service".equals(target)) {
            String serviceName = (String)iterator.next();
            printService(serviceName);

        } else if ("partition".equals(target)) {
            String partitionName = (String)iterator.next();
            printPartition(partitionName);

        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                printConnection(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                printSource(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                printModule(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processStartCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String name = (String)iterator.next();
            client.getServiceManagerClient().start(name);

        } else if ("partition".equals(target)) {
            String name = (String)iterator.next();
            client.getPartitionManagerClient().start(name);

        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getConnectionManagerClient().start(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getSourceManagerClient().start(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getModuleManagerClient().start(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processStopCommand(Iterator iterator) throws Exception {
        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String name = (String)iterator.next();
            client.getServiceManagerClient().stop(name);

        } else if ("partition".equals(target)) {
            String name = (String)iterator.next();
            client.getPartitionManagerClient().stop(name);

        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getConnectionManagerClient().stop(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getSourceManagerClient().stop(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getModuleManagerClient().stop(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void processRestartCommand(Iterator iterator) throws Exception {

        String target = (String)iterator.next();
        if ("service".equals(target)) {
            String name = (String)iterator.next();
            client.getServiceManagerClient().restart(name);

        } else if ("partition".equals(target)) {
            String name = (String)iterator.next();
            client.getPartitionManagerClient().restart(name);

        } else if ("connection".equals(target)) {
            String connectionName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getConnectionManagerClient().restart(partitionName, connectionName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("source".equals(target)) {
            String sourceName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getSourceManagerClient().restart(partitionName, sourceName);

            } else {
                System.out.println("Missing partition name");
            }

        } else if ("module".equals(target)) {
            String moduleName = (String)iterator.next();
            String partition = (String)iterator.next();

            if ("partition".equals(partition)) {
                String partitionName = (String)iterator.next();
                client.getModuleManagerClient().restart(partitionName, moduleName);

            } else {
                System.out.println("Missing partition name");
            }

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public void execute(Collection parameters) throws Exception {

        Iterator iterator = parameters.iterator();
        String command = (String)iterator.next();
        Client.log.debug("Executing "+command);

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

        } else if ("restart".equals(command)) {
            client.getServiceManagerClient().restart();

        } else if ("reload".equals(command)) {
            client.reload();

        } else if ("store".equals(command)) {
            client.store();

        } else if ("rename".equals(command)) {
            String object = (String)iterator.next();
            if ("entry".equals(object)) {
                String oldDn = (String)iterator.next();
                String newDn = (String)iterator.next();
                Client.log.debug("Renaming "+oldDn+" to "+newDn);
                client.renameEntryMapping(oldDn, newDn);
            }

        } else if ("loggers".equals(command)) {
            Collection loggerNames = client.getLoggerNames();
            for (Iterator i=loggerNames.iterator(); i.hasNext(); ) {
                String loggerName = (String)i.next();
                String l = client.getLoggerLevel(loggerName);

                System.out.println(loggerName+" ["+l +"]");
            }

        } else if ("logger".equals(command)) {
            String loggerName = (String)iterator.next();
            if (iterator.hasNext()) {
                String l = (String)iterator.next();
                client.setLoggerLevel(loggerName, "".equals(l) ? null : l);
            } else {
                String l = client.getLoggerLevel(loggerName);
                System.out.println(l);
            }

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.client.PenroseClient [OPTION]... <COMMAND>");
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
        System.out.println("  version");
        System.out.println();
        System.out.println("  show services");
        System.out.println("  show service <name>");
        System.out.println("  start service <name>");
        System.out.println("  stop service <name>");
        System.out.println("  restart service <name>");
        System.out.println();
        System.out.println("  show partitions");
        System.out.println("  show partition <name>");
        System.out.println("  start partition <name>");
        System.out.println("  stop partition <name>");
        System.out.println("  restart partition <name>");
        System.out.println();
        System.out.println("  show connections");
        System.out.println("  show connection <name> partition <name>");
        System.out.println("  start connection <name> partition <name>");
        System.out.println("  stop connection <name> partition <name>");
        System.out.println("  restart connection <name> partition <name>");
        System.out.println();
        System.out.println("  show sources");
        System.out.println("  show source <name> partition <name>");
        System.out.println("  start source <name> partition <name>");
        System.out.println("  stop source <name> partition <name>");
        System.out.println("  restart source <name> partition <name>");
        System.out.println();
        System.out.println("  show modules");
        System.out.println("  show module <name> partition <name>");
        System.out.println("  start module <name> partition <name>");
        System.out.println("  stop module <name> partition <name>");
        System.out.println("  restart module <name> partition <name>");
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

        Getopt getopt = new Getopt("PenroseClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection parameters = new ArrayList();
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
            Client.showUsage();
            System.exit(0);
        }

        String homeDirectory = System.getProperty("penrose.home");

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");
        File log4jXml = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.xml");

        if (level.equals(Level.DEBUG)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (level.equals(Level.INFO)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (log4jProperties.exists()) {
            PropertyConfigurator.configure(log4jProperties.getAbsolutePath());

        } else if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());

        } else {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);
        }

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
    }
}
