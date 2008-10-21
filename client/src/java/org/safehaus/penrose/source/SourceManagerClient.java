package org.safehaus.penrose.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.client.PenroseClient;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import javax.management.*;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class SourceManagerClient {

    public static Logger log = LoggerFactory.getLogger(SourceManagerClient.class);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showSources(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        System.out.print(TextUtil.rightPad("SOURCE", 40)+" ");
        System.out.println(TextUtil.leftPad("ENTRIES", 10));

        System.out.print(TextUtil.repeat("-", 40)+" ");
        System.out.println(TextUtil.repeat("-", 10));

        for (String sourceName : partitionClient.getSourceNames()) {

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);

            String entries;
            try {
                entries = ""+sourceClient.getCount();
            } catch (Exception e) {
                entries = "N/A";
            }

            System.out.print(TextUtil.rightPad(sourceName, 40)+" ");
            System.out.println(TextUtil.leftPad(entries, 10)+" ");
        }
    }

    public static void showSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        SourceConfig sourceConfig = sourceClient.getSourceConfig();

        System.out.println("Source      : "+sourceConfig.getName());
        System.out.println("Connection  : "+sourceConfig.getConnectionName());
        System.out.println("Partition   : "+partitionName);

        String description = sourceConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : sourceConfig.getParameterNames()) {
            String value = sourceConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }

        System.out.println();

        String entries;
        try {
            entries = ""+sourceClient.getCount();
        } catch (Exception e) {
            entries = "N/A";
        }

        System.out.println("Entries     : "+entries);
        System.out.println();

        System.out.println("Attributes:");
        for (MBeanAttributeInfo attributeInfo  : sourceClient.getAttributes()) {
            System.out.println(" - "+attributeInfo.getName()+" ("+attributeInfo.getType()+")");
        }
        System.out.println();

        System.out.println("Operations:");
        for (MBeanOperationInfo operationInfo : sourceClient.getOperations()) {

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
        if ("sources".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showSources(client, partitionName);

        } else if ("source".equals(target)) {
            String sourceName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showSource(client, partitionName, sourceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void createSources(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        for (String sourceName : partitionClient.getSourceNames()) {

            System.out.println("Creating source "+sourceName+" in partition "+partitionName+"...");

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
            sourceClient.create();

            System.out.println("Source created.");
        }
    }

    public static void createSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        System.out.println("Creating source "+sourceName+" in partition "+partitionName+"...");

        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        sourceClient.create();

        System.out.println("Source created.");
    }

    public static void processCreateCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("sources".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            createSources(client, partitionName);

        } else if ("source".equals(target)) {
            String sourceName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            createSource(client, partitionName, sourceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void clearSources(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        for (String sourceName : partitionClient.getSourceNames()) {

            System.out.println("Clearing source "+sourceName+" in partition "+partitionName+"...");

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
            sourceClient.clear();

            System.out.println("Source cleared.");
        }
    }

    public static void clearSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        System.out.println("Clearing source "+sourceName+" in partition "+partitionName+"...");

        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        sourceClient.clear();

        System.out.println("Source cleared.");
    }

    public static void processClearCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("sources".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            clearSources(client, partitionName);

        } else if ("source".equals(target)) {
            String sourceName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            clearSource(client, partitionName, sourceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void dropSources(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        for (String sourceName : partitionClient.getSourceNames()) {

            System.out.println("Dropping source "+sourceName+" in partition "+partitionName+"...");

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
            sourceClient.drop();

            System.out.println("Source dropped.");
        }
    }

    public static void dropSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        System.out.println("Dropping source "+sourceName+" in partition "+partitionName+"...");

        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        sourceClient.drop();

        System.out.println("Source dropped.");
    }

    public static void processDropCommand(PenroseClient client, Iterator<String> iterator) throws Exception {
        String target = iterator.next();
        if ("sources".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            dropSources(client, partitionName);

        } else if ("source".equals(target)) {
            String sourceName = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            dropSource(client, partitionName, sourceName);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String partitionName,
            String sourceName,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);

        Object returnValue = sourceClient.invoke(
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
        if ("source".equals(target)) {
            String sourceName = iterator.next();
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

            invokeMethod(client, partitionName, sourceName, methodName, paramValues, paramTypes);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void processSearchCommand(PenroseClient client, Iterator iterator) throws Exception {

        String target = (String)iterator.next();

        if ("source".equals(target)) {
            String partitionName = (String)iterator.next();
            String sourceName = (String)iterator.next();

            PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);

            SearchRequest request = new SearchRequest();
            SearchResponse response = new SearchResponse();

            sourceClient.search(request, response);

            System.out.println("Results:");
            while (response.hasNext()) {
                SearchResult result = response.next();
                System.out.println(" - "+result.getDn());
            }

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void execute(PenroseClient client, Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();
        System.out.println("Executing "+command);

        if ("show".equals(command)) {
            processShowCommand(client, iterator);

        } else if ("create".equals(command)) {
            processCreateCommand(client, iterator);

        } else if ("clear".equals(command)) {
            processClearCommand(client, iterator);

        } else if ("drop".equals(command)) {
            processDropCommand(client, iterator);

        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);

        } else if ("search".equals(command)) {
            processSearchCommand(client, iterator);

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.source.SourceManagerClient [OPTION]... <COMMAND>");
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
        System.out.println("  show sources in partition <partition name>");
        System.out.println("  show source <source name> in partition <partition name>>");
        System.out.println();
        System.out.println("  create sources in partition <partition name>");
        System.out.println("  create source <source name> in partition <partition name>");
        System.out.println();
        System.out.println("  clear sources in partition <partition name>");
        System.out.println("  clear source <source name> in partition <partition name>");
        System.out.println();
        System.out.println("  drop sources in partition <partition name>");
        System.out.println("  drop source <source name> in partition <partition name>");
        System.out.println();
        System.out.println("  invoke method <method name> in source <module name> in partition <partition name> [with <parameter>...]");
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

        Getopt getopt = new Getopt("SourceManagerClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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