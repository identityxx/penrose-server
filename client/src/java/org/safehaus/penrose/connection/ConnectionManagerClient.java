package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.ldap.DN;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import javax.management.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionManagerClient extends BaseClient implements ConnectionManagerServiceMBean {

    public static Logger log = LoggerFactory.getLogger(ConnectionManagerClient.class);

    protected String partitionName;

    public ConnectionManagerClient(PenroseClient client, String partitionName) throws Exception {
        super(client, "ConnectionManager", getStringObjectName(partitionName));

        this.partitionName = partitionName;
    }

    public static String getStringObjectName(String partitionName) {
        return "Penrose:type=connectionManager,partition="+partitionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ConnectionClient getConnectionClient(String connectionName) throws Exception {
        return new ConnectionClient(client, partitionName, connectionName);
    }

    public Collection<String> getConnectionNames() throws Exception {
        return (Collection<String>)getAttribute("ConnectionNames");
    }

    public void validateConnection(ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "validateConnection",
                new Object[] { connectionConfig },
                new String[] { ConnectionConfig.class.getName() }
        );
    }

    public Collection<DN> getNamingContexts(ConnectionConfig connectionConfig) throws Exception {
        return (Collection<DN>)invoke(
                "getNamingContexts",
                new Object[] { connectionConfig },
                new String[] { ConnectionConfig.class.getName() }
        );
    }

    public void createConnection(ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "createConnection",
                new Object[] { connectionConfig },
                new String[] { ConnectionConfig.class.getName() }
        );
    }

    public void renameConnection(String connectionName, String newConnectionName) throws Exception {
        invoke(
                "renameConnection",
                new Object[] { connectionName, newConnectionName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void updateConnection(String connectionName, ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "updateConnection",
                new Object[] { connectionName, connectionConfig },
                new String[] { String.class.getName(), ConnectionConfig.class.getName() }
        );
    }

    public void removeConnection(String connectionName) throws Exception {
        invoke(
                "removeConnection",
                new Object[] { connectionName },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showConnections(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        System.out.print(TextUtil.rightPad("CONNECTION", 40)+" ");
        System.out.println(TextUtil.rightPad("STATUS", 10));

        System.out.print(TextUtil.repeat("-", 40)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        for (String connectionName : connectionManagerClient.getConnectionNames()) {

            ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
            String status = connectionClient.getStatus();

            System.out.print(TextUtil.rightPad(connectionName, 40)+" ");
            System.out.println(TextUtil.rightPad(status, 10)+" ");
        }
    }

    public static void showConnection(PenroseClient client, String partitionName, String connectionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();
        ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
        ConnectionConfig connectionConfig = connectionClient.getConnectionConfig();

        System.out.println("Connection  : "+connectionConfig.getName());
        System.out.println("Partition   : "+partitionName);

        String description = connectionConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : connectionConfig.getParameterNames()) {
            String value = connectionConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
        System.out.println();

        System.out.println("Attributes:");
        for (MBeanAttributeInfo attributeInfo  : connectionClient.getAttributes()) {
            System.out.println(" - "+attributeInfo.getName()+" ("+attributeInfo.getType()+")");
        }
        System.out.println();

        System.out.println("Operations:");
        for (MBeanOperationInfo operationInfo : connectionClient.getOperations()) {

            Collection<String> paramTypes = new ArrayList<String>();
            for (MBeanParameterInfo parameterInfo : operationInfo.getSignature()) {
                paramTypes.add(parameterInfo.getType());
            }

            String operation = operationInfo.getReturnType()+" "+ ClassUtil.getSignature(operationInfo.getName(), paramTypes);
            System.out.println(" - "+operation);
        }
    }

    public static void processShowCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("connections".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showConnections(client, partitionName);

        } else if ("connection".equals(target)) {
            String connectionName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showConnection(client, partitionName, connectionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void startConnections(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        for (String connectionName : connectionManagerClient.getConnectionNames()) {

            System.out.println("Starting connection "+connectionName+" in partition "+partitionName+"...");

            ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
            connectionClient.start();

            System.out.println("Connection started.");
        }
    }

    public static void startConnection(PenroseClient client, String partitionName, String connectionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        System.out.println("Starting connection "+connectionName+" in partition "+partitionName+"...");

        ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
        connectionClient.start();

        System.out.println("Connection started.");
    }

    public static void processStartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("connections".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            startConnections(client, partitionName);

        } else if ("connection".equals(target)) {
            String connectionName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            startConnection(client, partitionName, connectionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void stopConnections(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        for (String connectionName : connectionManagerClient.getConnectionNames()) {

            System.out.println("Starting connection "+connectionName+" in partition "+partitionName+"...");

            ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
            connectionClient.stop();

            System.out.println("Connection started.");
        }
    }

    public static void stopConnection(PenroseClient client, String partitionName, String connectionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        System.out.println("Stopping connection "+connectionName+" in partition "+partitionName+"...");

        ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
        connectionClient.stop();

        System.out.println("Connection stopped.");
    }

    public static void processStopCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("connections".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            stopConnections(client, partitionName);

        } else if ("connection".equals(target)) {
            String connectionName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            stopConnection(client, partitionName, connectionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void restartConnections(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        for (String connectionName : connectionManagerClient.getConnectionNames()) {

            System.out.println("Restarting connection "+connectionName+" in partition "+partitionName+"...");

            ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
            connectionClient.restart();

            System.out.println("Connection restarted.");
        }
    }

    public static void restartConnection(PenroseClient client, String partitionName, String connectionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();

        System.out.println("Restarting connection "+connectionName+" in partition "+partitionName+"...");

        ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);
        connectionClient.restart();

        System.out.println("Connection restarted.");
    }

    public static void processRestartCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("connections".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            restartConnections(client, partitionName);

        } else if ("connection".equals(target)) {
            String connectionName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            restartConnection(client, partitionName, connectionName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String partitionName,
            String connectionName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        ConnectionManagerClient connectionManagerClient = partitionClient.getConnectionManagerClient();
        ConnectionClient connectionClient = connectionManagerClient.getConnectionClient(connectionName);

        Object returnValue = connectionClient.invoke(
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
        if ("connection".equals(target)) {
            String connectionName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
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

            invokeMethod(client, partitionName, connectionName, methodName, paramValues, paramTypes);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void execute(PenroseClient client, Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();
        //System.out.println("Executing "+command);

        if ("show".equals(command)) {
            processShowCommand(client, iterator);

        } else if ("start".equals(command)) {
            processStartCommand(client, iterator);

        } else if ("stop".equals(command)) {
            processStopCommand(client, iterator);

        } else if ("restart".equals(command)) {
            processRestartCommand(client, iterator);

        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.connection.ConnectionManagerClient [OPTION]... <COMMAND>");
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
        System.out.println("  show connections in partition <partition name>");
        System.out.println("  show connection <connection name> in partition <partition name>>");
        System.out.println();
        System.out.println("  start connections in partition <partition name>");
        System.out.println("  start connection <connection name> in partition <partition name>");
        System.out.println();
        System.out.println("  stop connections in partition <partition name>");
        System.out.println("  stop connection <connection name> in partition <partition name>");
        System.out.println();
        System.out.println("  restart connections in partition <partition name>");
        System.out.println("  restart connection <connection name> in partition <partition name>");
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

        Getopt getopt = new Getopt("ConnectionManagerClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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

        File serviceHome = new File(System.getProperty("org.safehaus.penrose.client.home"));

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.safehaus.penrose");

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