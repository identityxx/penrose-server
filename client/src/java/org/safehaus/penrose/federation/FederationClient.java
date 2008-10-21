package org.safehaus.penrose.federation;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.module.ModuleConfig;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class FederationClient implements FederationMBean {

    public static Logger log = Logger.getLogger(FederationClient.class);

    public final static String FEDERATION = "federation";
    public final static String GLOBAL     = "global";

    public final static String JDBC       = "JDBC";
    public final static String LDAP       = "LDAP";

    public final static String GLOBAL_PARAMETERS        = "global_parameters";
    public final static String REPOSITORIES             = "repositories";
    public final static String REPOSITORY_PARAMETERS    = "repository_parameters";

    String name;

    PenroseClient client;
    PartitionClient partitionClient;
    //ModuleClient moduleClient;

    public FederationClient(PenroseClient client, String name) throws Exception {
        this.client = client;
        this.name = name;

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        partitionClient = partitionManagerClient.getPartitionClient(name);

        //moduleClient = partitionClient.getModuleClient("FederationModule");
    }

    public String getName() {
        return name;
    }

    public PenroseClient getClient() {
        return client;
    }
    
    public PartitionClient getPartitionClient() {
        return partitionClient;
    }
    
    public void install() throws Exception {

        ModuleConfig moduleConfig = new ModuleConfig();
        moduleConfig.setName("FederationModule");
        moduleConfig.setModuleClass("org.safehaus.penrose.federation.module.FederationModule");

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient("DEFAULT");

        partitionClient.createModule(moduleConfig);
        partitionClient.store();
    }

    public void uninstall() throws Exception {

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient("DEFAULT");

        partitionClient.removeModule("FederationModule");
        partitionClient.store();
    }

    public boolean isInstalled() throws Exception {
        return partitionClient.exists();
    }

    public PartitionClient getFederationPartitionClient() {
        return partitionClient;
    }
    
    public FederationRepositoryConfig getGlobalRepository() throws Exception {
        return getRepository(GLOBAL);
    }

    public void updateGlobalRepository(FederationRepositoryConfig repository) throws Exception {
        partitionClient.invoke(
                "updateGlobalRepository",
                new Object[] { repository },
                new String[] { FederationRepositoryConfig.class.getName() }
        );
    }

    public void addRepository(FederationRepositoryConfig repository) throws Exception {
        partitionClient.invoke(
                "addRepository",
                new Object[] { repository },
                new String[] { FederationRepositoryConfig.class.getName() }
        );
    }

    public void removeRepository(String name) throws Exception {
        partitionClient.invoke(
                "removeRepository",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public FederationConfig getFederationConfig() throws Exception {
        return (FederationConfig)partitionClient.getAttribute("FederationConfig");
    }

    public void setFederationConfig(FederationConfig federationConfig) throws Exception {
        partitionClient.setAttribute("FederationConfig", federationConfig);
    }

    public void storeFederationConfig() throws Exception {
        partitionClient.invoke("store");
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return (Collection)partitionClient.invoke(
                "getRepositoryNames",
                new Object[] { },
                new String[] { }
        );
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return (Collection)partitionClient.invoke(
                "getRepositories",
                new Object[] { },
                new String[] { }
        );
    }

    public Collection<FederationRepositoryConfig> getRepositories(String type) throws Exception {
        return (Collection)partitionClient.invoke(
                "getRepositories",
                new Object[] { type },
                new String[] { String.class.getName() }
        );
    }

    public FederationRepositoryConfig getRepository(String name) throws Exception {
        return (FederationRepositoryConfig)partitionClient.invoke(
                "getRepository",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public Collection<String> getPartitionNames() throws Exception {
        return (Collection)partitionClient.invoke(
                "getPartitionNames",
                new Object[] { },
                new String[] { }
        );
    }

    public Collection<FederationPartitionConfig> getPartitions() throws Exception {
        return (Collection)partitionClient.invoke(
                "getPartitions",
                new Object[] { },
                new String[] { }
        );
    }

    public FederationPartitionConfig getPartition(String name) throws Exception {
        return (FederationPartitionConfig)partitionClient.invoke(
                "getPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createPartitions() throws Exception {
        partitionClient.invoke(
                "createPartitions",
                new Object[] { },
                new String[] { }
        );
    }

    public void startPartitions() throws Exception {
        partitionClient.invoke(
                "createPartitions",
                new Object[] { },
                new String[] { }
        );
    }

    public void createPartition(String name) throws Exception {
        partitionClient.invoke(
                "createPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void startPartition(String name) throws Exception {
        partitionClient.invoke(
                "startPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stopPartitions() throws Exception {
        partitionClient.invoke(
                "removePartitions",
                new Object[] { },
                new String[] { }
        );
    }

    public void removePartitions() throws Exception {
        partitionClient.invoke(
                "removePartitions",
                new Object[] { },
                new String[] { }
        );
    }

    public void stopPartition(String name) throws Exception {
        partitionClient.invoke(
                "stopPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removePartition(String name) throws Exception {
        partitionClient.invoke(
                "removePartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void synchronize(String name) throws Exception {
        partitionClient.invoke(
                "synchronize",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public static void execute(PenroseClient client, Collection<String> commands) throws Exception {

        Iterator<String> iterator = commands.iterator();
        String command = iterator.next();

        if ("status".equals(command)) {
            String partition = iterator.next();
            FederationClient federationClient = new FederationClient(client, partition);

            if (federationClient.isInstalled()) {
                System.out.println("Federation module is installed.");

                Collection<String> repositoryNames = federationClient.getRepositoryNames();

                if (repositoryNames.isEmpty()) {
                    System.out.println("There are no repositories.");

                } else {
                    System.out.println("Repositories:");
                    for (String name : repositoryNames) {
                        System.out.println(" - "+name);
                    }
                }

            } else {
                System.out.println("Federation module is not installed.");
            }

        } else if ("install".equals(command)) {
            System.out.println("Installing Federation module...");
            String partition = iterator.next();
            FederationClient federationClient = new FederationClient(client, partition);
            federationClient.install();
            System.out.println("Done.");

        } else if ("uninstall".equals(command)) {
            System.out.println("Uninstalling Federation module...");
            String partition = iterator.next();
            FederationClient federationClient = new FederationClient(client, partition);
            federationClient.uninstall();
            System.out.println("Done.");

        } else if ("createPartitions".equals(command)) {
            String partition = iterator.next();
            FederationClient federationClient = new FederationClient(client, partition);

            Collection<String> repositoryNames;
            if (iterator.hasNext()) {
                repositoryNames = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String repository = iterator.next();
                    repositoryNames.add(repository);
                }

            } else {
                repositoryNames = federationClient.getRepositoryNames();
            }

            for (String repository : repositoryNames) {
                System.out.println("Creating partitions for "+repository+"...");
                federationClient.createPartition(repository);
                federationClient.startPartition(repository);
            }

            System.out.println("Done.");

        } else if ("removePartitions".equals(command)) {

            String partition = iterator.next();
            FederationClient federationClient = new FederationClient(client, partition);

            Collection<String> repositoryNames;
            if (iterator.hasNext()) {
                repositoryNames = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String repository = iterator.next();
                    repositoryNames.add(repository);
                }

            } else {
                repositoryNames = federationClient.getRepositoryNames();
            }

            for (String repository : repositoryNames) {
                System.out.println("Removing partitions for "+repository+"...");
                federationClient.stopPartition(repository);
                federationClient.removePartition(repository);
            }

            System.out.println("Done.");

        } else if ("synchronize".equals(command)) {

            String partition = iterator.next();
            FederationClient federationClient = new FederationClient(client, partition);

            Collection<String> repositoryNames;
            if (iterator.hasNext()) {
                repositoryNames = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String repository = iterator.next();
                    repositoryNames.add(repository);
                }

            } else {
                repositoryNames = federationClient.getRepositoryNames();
            }

            for (String repository : repositoryNames) {
                System.out.println("Synchronizing "+repository+"...");
                federationClient.synchronize(repository);
            }

            System.out.println("Done.");

        } else {
            throw new Exception("Unknown command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.federation.FederationClient [OPTION]... <command> [arguments]...");
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
        System.out.println("  install <partition>                                   Install Federation module on the server.");
        System.out.println("  uninstall <partition>                                 Uninstall Federation module from the server.");
        System.out.println("  status <partition>                                    Display Federation status.");
        System.out.println("  createPartitions <partition> [repository...]          Create partitions for this repository");
        System.out.println("  removePartitions <partition> [repository...]          Remove partitions for this repository");
        System.out.println("  synchronize <partition> [repository...]               Synchronize this repository.");
    }

    public static void main(String args[]) throws Exception {

        Level level          = Level.WARN;
        String serverType    = PenroseClient.PENROSE;
        String protocol      = PenroseClient.DEFAULT_PROTOCOL;
        String hostname      = "localhost";
        int port             = PenroseClient.DEFAULT_RMI_PORT;
        int rmiTransportPort = PenroseClient.DEFAULT_RMI_TRANSPORT_PORT;

        String bindDn = null;
        String bindPassword = null;

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("FederationClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection<String> commands = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    showUsage();
                    System.exit(0);
                    break;
                case 1:
                    commands.add(getopt.getOptarg());
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
                    port = Integer.parseInt(getopt.getOptarg());
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

        if (commands.size() == 0) {
            showUsage();
            System.exit(0);
        }

        File penroseHome = new File(System.getProperty("org.safehaus.penrose.client.home"));

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");

        File log4jXml = new File(penroseHome, "conf"+File.separator+"log4j.xml");

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
                    port,
                    bindDn,
                    bindPassword
            );

            client.setRmiTransportPort(rmiTransportPort);
            client.connect();

            execute(client, commands);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
