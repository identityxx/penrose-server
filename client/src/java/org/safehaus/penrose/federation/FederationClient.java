package org.safehaus.penrose.federation;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.module.ModuleClient;
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

    PenroseClient client;
    ModuleClient moduleClient;

    public FederationClient(PenroseClient client) throws Exception {
        this.client = client;

        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient("DEFAULT");

        moduleClient = partitionClient.getModuleClient("FederationModule");

        if (!isInstalled()) {
            install();
        }
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
        return moduleClient.exists();
    }

    public ModuleClient getFederationModuleClient() {
        return moduleClient;
    }
    
    public GlobalRepository getGlobalRepository() throws Exception {
        return (GlobalRepository)getRepository(GLOBAL);
    }

    public void updateGlobalRepository(Repository repository) throws Exception {
        moduleClient.invoke(
                "updateGlobalRepository",
                new Object[] { repository },
                new String[] { Repository.class.getName() }
        );
    }

    public void addRepository(Repository repository) throws Exception {
        moduleClient.invoke(
                "addRepository",
                new Object[] { repository },
                new String[] { Repository.class.getName() }
        );
    }

    public void removeRepository(String name) throws Exception {
        moduleClient.invoke(
                "removeRepository",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public FederationConfig getFederationConfig() throws Exception {
        return (FederationConfig)moduleClient.getAttribute("FederationConfig");
    }

    public void setFederationConfig(FederationConfig federationConfig) throws Exception {
        moduleClient.setAttribute("FederationConfig", federationConfig);
    }

    public void storeFederationConfig() throws Exception {
        moduleClient.invoke("store");
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return (Collection)moduleClient.invoke(
                "getRepositoryNames",
                new Object[] { },
                new String[] { }
        );
    }

    public Collection<Repository> getRepositories() throws Exception {
        return (Collection)moduleClient.invoke(
                "getRepositories",
                new Object[] { },
                new String[] { }
        );
    }

    public Collection<Repository> getRepositories(String type) throws Exception {
        return (Collection)moduleClient.invoke(
                "getRepositories",
                new Object[] { type },
                new String[] { String.class.getName() }
        );
    }

    public Repository getRepository(String name) throws Exception {
        return (Repository)moduleClient.invoke(
                "getRepository",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createPartitions(String name) throws Exception {
        moduleClient.invoke(
                "createPartitions",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removePartitions(String name) throws Exception {
        moduleClient.invoke(
                "removePartitions",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void synchronize(String name) throws Exception {
        moduleClient.invoke(
                "synchronize",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void synchronize(String name, Collection<String> parameters) throws Exception {
        moduleClient.invoke(
                "synchronize",
                new Object[] { name, parameters },
                new String[] { String.class.getName(), Collection.class.getName() }
        );
    }

    public void execute(Collection<String> commands) throws Exception {

        Iterator<String> iterator = commands.iterator();
        String command = iterator.next();

        if ("status".equals(command)) {
            if (isInstalled()) {
                System.out.println("Federation module is installed.");

                Collection<String> repositoryNames = getRepositoryNames();

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
            install();
            System.out.println("Federation module has been installed.");

        } else if ("uninstall".equals(command)) {
            uninstall();
            System.out.println("Federation module has been uninstalled.");

        } else if ("createPartitions".equals(command)) {
            String repository = iterator.next();
            createPartitions(repository);

        } else if ("removePartitions".equals(command)) {
            String repository = iterator.next();
            removePartitions(repository);

        } else if ("synchronize".equals(command)) {
            String name = iterator.next();
            System.out.println("Synchronizing "+name+"...");

            Collection<String> parameters = new ArrayList<String>();
            while (iterator.hasNext()) {
                String parameter = iterator.next();
                parameters.add(parameter);
            }

            synchronize(name, parameters);
            System.out.println("Synchronization completed.");

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
        System.out.println("  install                                   Install Federation module on the server.");
        System.out.println("  uninstall                                 Uninstall Federation module from the server.");
        System.out.println("  status                                    Display Federation status.");
        System.out.println("  createPartitions <repository>             Create partitions for this repository");
        System.out.println("  createRemove <repository>                 Remove partitions for this repository");
        System.out.println("  synchronize <repository> [parameters...]  Synchronize repository.");
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

        Getopt getopt = new Getopt("Client", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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

        File penroseHome = new File(System.getProperty("penrose.home"));

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

            FederationClient federationClient = new FederationClient(client);
            federationClient.execute(commands);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
