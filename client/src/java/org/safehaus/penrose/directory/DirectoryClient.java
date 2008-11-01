package org.safehaus.penrose.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
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
public class DirectoryClient extends BaseClient implements DirectoryServiceMBean {

    public static Logger log = LoggerFactory.getLogger(DirectoryClient.class);

    protected String partitionName;

    public DirectoryClient(PenroseClient client, String partitionName) throws Exception {
        super(client, "Directory", getStringObjectName(partitionName));

        this.partitionName = partitionName;
    }

    public DirectoryConfig getDirectoryConfig() throws Exception {
        return (DirectoryConfig)getAttribute("DirectoryConfig");
    }

    public static String getStringObjectName(String partitionName) {
        return "Penrose:type=directory,partition="+partitionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Entries
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DN getSuffix() throws Exception {
        return (DN)getAttribute("Suffix");
    }

    public Collection<DN> getSuffixes() throws Exception {
        return (Collection<DN>)getAttribute("Suffixes");
    }

    public Collection<String> getRootEntryIds() throws Exception {
        return (Collection<String>)getAttribute("RootEntryIds");
    }

    public Collection<String> getEntryIds() throws Exception {
        return (Collection<String>)getAttribute("EntryIds");
    }

    public EntryClient getEntryClient(String entryId) throws Exception {
        return new EntryClient(client, partitionName, entryId);
    }

    public String createEntry(EntryConfig entryConfig) throws Exception {
        return (String)invoke(
                "createEntry",
                new Object[] { entryConfig },
                new String[] { EntryConfig.class.getName() }
        );
    }

    public void updateEntry(String id, EntryConfig entryConfig) throws Exception {
        invoke(
                "updateEntry",
                new Object[] { id, entryConfig },
                new String[] { String.class.getName(), EntryConfig.class.getName() }
        );
    }

    public void removeEntry(String id) throws Exception {
        invoke(
                "removeEntry",
                new Object[] { id },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showEntries(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        DirectoryClient directoryClient = partitionClient.getDirectoryClient();

        System.out.print(TextUtil.rightPad("ENTRIES", 10)+" ");
        System.out.println(TextUtil.rightPad("DN", 50));

        System.out.print(TextUtil.repeat("-", 10)+" ");
        System.out.println(TextUtil.repeat("-", 50));

        for (String entryId : directoryClient.getEntryIds()) {

            EntryClient entryClient = directoryClient.getEntryClient(entryId);
            DN dn = entryClient.getDn();

            System.out.print(TextUtil.rightPad(entryId, 10)+" ");
            System.out.println(TextUtil.rightPad(dn.toString(), 50)+" ");
        }
    }

    public static void showEntry(PenroseClient client, String partitionName, String entryId) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        DirectoryClient directoryClient = partitionClient.getDirectoryClient();
        EntryClient entryClient = directoryClient.getEntryClient(entryId);
        EntryConfig entryConfig = entryClient.getEntryConfig();

        System.out.println("ID          : "+entryConfig.getId());
        System.out.println("DN          : "+entryConfig.getDn());

        String entryClass = entryConfig.getEntryClass();
        System.out.println("Class       : "+(entryClass == null ? "" : entryClass));

        String description = entryConfig.getDescription();
        System.out.println("Description : "+(description == null ? "" : description));
        System.out.println();

        String parentId = entryClient.getParentId();
        System.out.println("Parent ID   : "+(parentId == null ? "" : parentId));

        Collection<String> childIds = entryClient.getChildIds();
        System.out.println("Child IDs   : "+childIds);
        System.out.println();

        System.out.println("Parameters  :");
        for (String paramName : entryConfig.getParameterNames()) {
            String value = entryConfig.getParameter(paramName);
            System.out.println(" - " + paramName + ": " + value);
        }
        System.out.println();

        System.out.println("Attributes:");
        for (MBeanAttributeInfo attributeInfo  : entryClient.getAttributes()) {
            System.out.println(" - "+attributeInfo.getName()+" ("+attributeInfo.getType()+")");
        }
        System.out.println();

        System.out.println("Operations:");
        for (MBeanOperationInfo operationInfo : entryClient.getOperations()) {

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
        if ("entries".equals(target)) {
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showEntries(client, partitionName);

        } else if ("entry".equals(target)) {
            String entryId = iterator.next();
            iterator.next(); // in
            iterator.next(); // partition
            String partitionName = iterator.next();
            showEntry(client, partitionName, entryId);

        } else {
            System.out.println("Invalid target: "+target);
        }
    }

    public static void invokeMethod(
            PenroseClient client,
            String partitionName,
            String entryId,
            String methodName,
            Object[] paramValues,
            String[] paramTypes
    ) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        DirectoryClient directoryClient = partitionClient.getDirectoryClient();
        EntryClient entryClient = directoryClient.getEntryClient(entryId);

        Object returnValue = entryClient.invoke(
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
        if ("entry".equals(target)) {
            String entryId = iterator.next();
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

            invokeMethod(client, partitionName, entryId, methodName, paramValues, paramTypes);

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

        } else if ("invoke".equals(command)) {
            processInvokeCommand(client, iterator);

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.directory.DirectoryClient [OPTION]... <COMMAND>");
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
        System.out.println("  show entries in partition <partition name>");
        System.out.println("  show entry <entry ID> in partition <partition name>>");
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

        Getopt getopt = new Getopt("ConnectionClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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