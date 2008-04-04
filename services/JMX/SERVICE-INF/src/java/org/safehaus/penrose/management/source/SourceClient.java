package org.safehaus.penrose.management.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.management.partition.PartitionManagerClient;
import org.safehaus.penrose.management.partition.PartitionClient;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.Client;
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
public class SourceClient extends BaseClient implements SourceServiceMBean {

    public static Logger log = LoggerFactory.getLogger(SourceClient.class);

    protected String partitionName;

    public SourceClient(PenroseClient client, String partitionName, String name) throws Exception {
        super(client, name, getStringObjectName(partitionName, name));

        this.partitionName = partitionName;
    }
    
    public Long getCount() throws Exception {
        return (Long)connection.getAttribute(objectName, "Count");
    }

    public void create() throws Exception {
        invoke("create", new Object[] {}, new String[] {});
    }

    public void clear() throws Exception {
        invoke("clear", new Object[] {}, new String[] {});
    }

    public void drop() throws Exception {
        invoke("drop", new Object[] {}, new String[] {});
    }

    public String getAdapterName() throws Exception {
        return (String)connection.getAttribute(objectName, "AdapterName");
    }

    public SourceConfig getSourceConfig() throws Exception {
        return (SourceConfig)connection.getAttribute(objectName, "SourceConfig");
    }

    public static String getStringObjectName(String partitionName, String sourceName) {
        return "Penrose:type=source,partition="+partitionName+",name="+sourceName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getConnectionName() throws Exception {
        return (String)getAttribute("ConnectionName");
    }

    public String getParameter(String name) throws Exception {
        return (String)invoke(
                "getParameter",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public Collection<String> getParameterNames() throws Exception {
        return (Collection<String>)invoke(
                "getParameterNames",
                new Object[] { },
                new String[] { }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(
            String dn,
            Attributes attributes
    ) throws Exception {
        return (AddResponse)invoke(
                "add",
                new Object[] { dn, attributes },
                new String[] { String.class.getName(), Attributes.class.getName() }
        );
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {
        return (AddResponse)invoke(
                "add",
                new Object[] { rdn, attributes },
                new String[] { RDN.class.getName(), Attributes.class.getName() }
        );
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {
        return (AddResponse)invoke(
                "add",
                new Object[] { dn, attributes },
                new String[] { DN.class.getName(), Attributes.class.getName() }
        );
    }

    public AddResponse add(
            AddRequest request,
            AddResponse response
    ) throws Exception {
        AddResponse newResponse = (AddResponse)invoke(
                "add",
                new Object[] { request, response },
                new String[] { AddRequest.class.getName(), AddResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            String dn
    ) throws Exception {
        return (DeleteResponse)invoke(
                "delete",
                new Object[] { dn },
                new String[] { String.class.getName() }
        );
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {
        return (DeleteResponse)invoke(
                "delete",
                new Object[] { rdn },
                new String[] { RDN.class.getName() }
        );
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {
        return (DeleteResponse)invoke(
                "delete",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    public DeleteResponse delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        DeleteResponse newResponse = (DeleteResponse)invoke(
                "delete",
                new Object[] { request, response },
                new String[] { DeleteRequest.class.getName(), DeleteResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            String dn
    ) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { dn },
                new String[] { String.class.getName() }
        );
    }

    public SearchResult find(
            RDN rdn
    ) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { rdn },
                new String[] { RDN.class.getName() }
        );
    }

    public SearchResult find(
            DN dn
    ) throws Exception {
        return (SearchResult)invoke(
                "find",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {
        return (ModifyResponse)invoke(
                "modify",
                new Object[] { dn, modifications },
                new String[] { String.class.getName(), Collection.class.getName() }
        );
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {
        return (ModifyResponse)invoke(
                "modify",
                new Object[] { rdn, modifications },
                new String[] { RDN.class.getName(), Collection.class.getName() }
        );
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {
        return (ModifyResponse)invoke(
                "modify",
                new Object[] { dn, modifications },
                new String[] { DN.class.getName(), Collection.class.getName() }
        );
    }

    public ModifyResponse modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        ModifyResponse newResponse = (ModifyResponse)invoke(
                "modify",
                new Object[] { request, response },
                new String[] { ModifyRequest.class.getName(), ModifyResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { dn, newRdn, deleteOldRdn },
                new String[] { String.class.getName(), String.class.getName(), boolean.class.getName() }
        );
    }

    public ModRdnResponse modrdn(
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { rdn, newRdn, deleteOldRdn },
                new String[] { RDN.class.getName(), RDN.class.getName(), boolean.class.getName() }
        );
    }

    public ModRdnResponse modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { dn, newRdn, deleteOldRdn },
                new String[] { DN.class.getName(), RDN.class.getName(), boolean.class.getName() }
        );
    }

    public ModRdnResponse modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        ModRdnResponse newResponse = (ModRdnResponse)invoke(
                "modrdn",
                new Object[] { request, response },
                new String[] { ModRdnRequest.class.getName(), ModRdnResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            String dn,
            String filter,
            int scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { dn, filter, scope },
                new String[] { String.class.getName(), String.class.getName(), int.class.getName() }
        );
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            int scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { rdn, filter, scope },
                new String[] { RDN.class.getName(), Filter.class.getName(), int.class.getName() }
        );
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { dn, filter, scope },
                new String[] { DN.class.getName(), Filter.class.getName(), int.class.getName() }
        );
    }

    public SearchResponse search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        SearchResponse newResponse = (SearchResponse)invoke(
                "search",
                new Object[] { request, response },
                new String[] { SearchRequest.class.getName(), SearchResponse.class.getName() }
        );
        response.copy(newResponse);
        return response;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Command Line
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void showSources(PenroseClient client, String partitionName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        System.out.print(org.safehaus.penrose.util.Formatter.rightPad("SOURCE", 40)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.leftPad("ENTRIES", 10));

        System.out.print(org.safehaus.penrose.util.Formatter.repeat("-", 40)+" ");
        System.out.println(org.safehaus.penrose.util.Formatter.repeat("-", 10));

        for (String sourceName : partitionClient.getSourceNames()) {

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);

            String entries;
            try {
                entries = ""+sourceClient.getCount();
            } catch (Exception e) {
                entries = "N/A";
            }

            System.out.print(org.safehaus.penrose.util.Formatter.rightPad(sourceName, 40)+" ");
            System.out.println(org.safehaus.penrose.util.Formatter.leftPad(entries, 10)+" ");
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

            log.debug("Creating source "+sourceName+" in partition "+partitionName+"...");

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
            sourceClient.create();

            log.debug("Source created.");
        }
    }

    public static void createSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        log.debug("Creating source "+sourceName+" in partition "+partitionName+"...");

        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        sourceClient.create();

        log.debug("Source created.");
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

            log.debug("Clearing source "+sourceName+" in partition "+partitionName+"...");

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
            sourceClient.clear();

            log.debug("Source cleared.");
        }
    }

    public static void clearSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        log.debug("Clearing source "+sourceName+" in partition "+partitionName+"...");

        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        sourceClient.clear();

        log.debug("Source cleared.");
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

            log.debug("Dropping source "+sourceName+" in partition "+partitionName+"...");

            SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
            sourceClient.drop();

            log.debug("Source dropped.");
        }
    }

    public static void dropSource(PenroseClient client, String partitionName, String sourceName) throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);

        log.debug("Dropping source "+sourceName+" in partition "+partitionName+"...");

        SourceClient sourceClient = partitionClient.getSourceClient(sourceName);
        sourceClient.drop();

        log.debug("Source dropped.");
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
        log.debug("Executing "+command);

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
        System.out.println("Usage: org.safehaus.penrose.management.source.SourceClient [OPTION]... <COMMAND>");
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

        Getopt getopt = new Getopt("SourceClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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
