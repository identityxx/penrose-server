package org.safehaus.penrose.partition;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntryClient;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.Client;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.partition.PartitionServiceMBean;
import org.safehaus.penrose.connection.ConnectionClient;
import org.safehaus.penrose.mapping.MappingClient;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.scheduler.SchedulerClient;
import org.safehaus.penrose.source.SourceClient;
import org.safehaus.penrose.mapping.MappingConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.util.TextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionClient extends BaseClient implements PartitionServiceMBean {

    public static Logger log = LoggerFactory.getLogger(PartitionClient.class);

    public PartitionClient(PenroseClient client, String name) throws Exception {
        super(client, name, getStringObjectName(name));
    }

    public static String getStringObjectName(String name) {
        return "Penrose:type=partition,name="+name;
    }

    public void start() throws Exception {
        invoke(
                "start",
                new Object[] { },
                new String[] { });
    }

    public void stop() throws Exception {
        invoke(
                "stop",
                new Object[] { },
                new String[] { });
    }

    public String getStatus() throws Exception {
        return (String)getAttribute("Status");
    }

    public PartitionConfig getPartitionConfig() throws Exception {
        return (PartitionConfig)getAttribute("PartitionConfig");
    }

    public void store() throws Exception {
        invoke(
                "store",
                new Object[] { },
                new String[] { }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getConnectionNames() throws Exception {
        return (Collection<String>)getAttribute("ConnectionNames");
    }

    public ConnectionClient getConnectionClient(String connectionName) throws Exception {
        return new ConnectionClient(client, name, connectionName);
    }

    public void createConnection(ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "createConnection",
                new Object[] { connectionConfig },
                new String[] { ConnectionConfig.class.getName() }
        );
    }

    public void updateConnection(String name, ConnectionConfig connectionConfig) throws Exception {
        invoke(
                "updateConnection",
                new Object[] { name, connectionConfig },
                new String[] { String.class.getName(), ConnectionConfig.class.getName() }
        );
    }

    public void removeConnection(String name) throws Exception {
        invoke(
                "removeConnection",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() throws Exception {
        return (Collection<String>)getAttribute("SourceNames");
    }

    public SourceClient getSourceClient(String sourceName) throws Exception {
        return new SourceClient(client, name, sourceName);
    }

    public void createSource(SourceConfig sourceConfig) throws Exception {
        invoke(
                "createSource",
                new Object[] { sourceConfig },
                new String[] { SourceConfig.class.getName() }
        );
    }

    public void updateSource(String name, SourceConfig sourceConfig) throws Exception {
        invoke(
                "updateSource",
                new Object[] { name, sourceConfig },
                new String[] { String.class.getName(), SourceConfig.class.getName() }
        );
    }

    public void removeSource(String name) throws Exception {
        invoke(
                "removeSource",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Mappings
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getMappingNames() throws Exception {
        return (Collection<String>)getAttribute("MappingNames");
    }

    public MappingClient getMappingClient(String mappingName) throws Exception {
        return new MappingClient(client, name, mappingName);
    }

    public void createMapping(MappingConfig mappingConfig) throws Exception {
        invoke(
                "createMapping",
                new Object[] { mappingConfig },
                new String[] { MappingConfig.class.getName() }
        );
    }

    public void updateMapping(String mappingName, MappingConfig mappingConfig) throws Exception {
        invoke(
                "updateMapping",
                new Object[] { mappingName, mappingConfig },
                new String[] { String.class.getName(), MappingConfig.class.getName() }
        );
    }

    public void removeMapping(String mappingName) throws Exception {
        invoke(
                "removeMapping",
                new Object[] { mappingName },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Directory
    ////////////////////////////////////////////////////////////////////////////////

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
        return new EntryClient(client, name, entryId);
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

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() throws Exception {
        return (Collection<String>)getAttribute("ModuleNames");
    }

    public ModuleClient getModuleClient(String moduleName) throws Exception {
        return new ModuleClient(client, name, moduleName);
    }

    public void createModule(ModuleConfig moduleConfig) throws Exception {
        invoke(
                "createModule",
                new Object[] { moduleConfig },
                new String[] { ModuleConfig.class.getName() }
        );
    }

    public void createModule(ModuleConfig moduleConfig, Collection<ModuleMapping> moduleMappings) throws Exception {
        invoke(
                "createModule",
                new Object[] { moduleConfig, moduleMappings },
                new String[] { ModuleConfig.class.getName(), Collection.class.getName() }
        );
    }

    public void updateModule(String name, ModuleConfig moduleConfig) throws Exception {
        invoke(
                "updateModule",
                new Object[] { name, moduleConfig },
                new String[] { String.class.getName(), ModuleConfig.class.getName() }
        );
    }

    public void removeModule(String name) throws Exception {
        invoke(
                "removeModule",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Scheduler
    ////////////////////////////////////////////////////////////////////////////////

    public SchedulerClient getSchedulerClient() throws Exception {
        return new SchedulerClient(client, name);
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
            Integer scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { dn, filter, scope },
                new String[] { String.class.getName(), String.class.getName(), Integer.class.getName() }
        );
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            Integer scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { rdn, filter, scope },
                new String[] { RDN.class.getName(), Filter.class.getName(), Integer.class.getName() }
        );
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            Integer scope
    ) throws Exception {
        return (SearchResponse)invoke(
                "search",
                new Object[] { dn, filter, scope },
                new String[] { DN.class.getName(), Filter.class.getName(), Integer.class.getName() }
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

    public static void showPartitions(PenroseClient client) throws Exception {
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

    public static void showPartition(PenroseClient client, String partitionName) throws Exception {

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

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        for (String partitionName : partitionManagerClient.getPartitionNames()) {

            log.debug("Starting partition "+partitionName+"...");

            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            //partitionClient.start();

            log.debug("Partition started.");
        }
    }

    public static void startPartition(PenroseClient client, String partitionName) throws Exception {

        log.debug("Starting partition "+partitionName+"...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        //partitionClient.start();

        log.debug("Partition started.");
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

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        for (String partitionName : partitionManagerClient.getPartitionNames()) {

            log.debug("Stopping partition "+partitionName+"...");

            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            //partitionClient.stop();

            log.debug("Partition stopped.");
        }
    }

    public static void stopPartition(PenroseClient client, String partitionName) throws Exception {

        log.debug("Stoping partition "+partitionName+"...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        //partitionClient.stop();

        log.debug("Partition stopped.");
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

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        for (String partitionName : partitionManagerClient.getPartitionNames()) {

            log.debug("Restarting partition "+partitionName+"...");

            PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
            //partitionClient.restart();

            log.debug("Partition restarted.");
        }
    }

    public static void restartPartition(PenroseClient client, String partitionName) throws Exception {

        log.debug("Restarting partition "+partitionName+"...");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(partitionName);
        //partitionClient.restart();

        log.debug("Partition restarted.");
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
        System.out.println("Usage: org.safehaus.penrose.partition.PartitionClient [OPTION]... <COMMAND>");
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

        Getopt getopt = new Getopt("PartitionClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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
