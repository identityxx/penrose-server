package org.safehaus.penrose.federation;

import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.federation.SynchronizationResult;
import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;

import java.util.*;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class NISFederationClient {

    public static Logger log = Logger.getLogger(NISFederationClient.class);

    public final static String NIS_TOOL              = "nis_tool";

    public final static String CACHE_USERS           = "cache_users";
    public final static String CACHE_GROUPS          = "cache_groups";
    public final static String CACHE_CONNECTION_NAME = "Cache";

    public final static String CHANGE_USERS          = "change_users";
    public final static String CHANGE_GROUPS         = "change_groups";

    public final static String LDAP_CONNECTION_NAME  = "LDAP";

    FederationClient federation;
    ModuleClient moduleClient;

    public NISFederationClient(FederationClient federation) {
        this.federation = federation;
        moduleClient = federation.getFederationModuleClient();
    }

    public void createYPPartition(String name) throws Exception {
        moduleClient.invoke(
                "createYPPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void startYPPartition(String name) throws Exception {
        moduleClient.invoke(
                "startYPPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stopYPPartition(String name) throws Exception {
        moduleClient.invoke(
                "stopYPPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removeYPPartition(String name) throws Exception {
        moduleClient.invoke(
                "removeYPPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createNISPartition(String name) throws Exception {
        moduleClient.invoke(
                "createNISPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void startNISPartition(String name) throws Exception {
        moduleClient.invoke(
                "startNISPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stopNISPartition(String name) throws Exception {
        moduleClient.invoke(
                "stopNISPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removeNISPartition(String name) throws Exception {
        moduleClient.invoke(
                "removeNISPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void createNSSPartition(String name) throws Exception {
        moduleClient.invoke(
                "createNSSPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void startNSSPartition(String name) throws Exception {
        moduleClient.invoke(
                "startNSSPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stopNSSPartition(String name) throws Exception {
        moduleClient.invoke(
                "stopNSSPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void removeNSSPartition(String name) throws Exception {
        moduleClient.invoke(
                "removeNSSPartition",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void addRepository(NISDomain repository) throws Exception {
        federation.addRepository(repository);
        federation.storeFederationConfig();
    }

    public void updateRepository(NISDomain repository) throws Exception {

        String name = repository.getName();
        federation.stopPartitions(name);
        federation.removePartitions(name);

        federation.removeRepository(name);
        federation.addRepository(repository);
        federation.storeFederationConfig();

        federation.createPartitions(name);
        federation.startPartitions(name);
    }

    public void removeRepository(String name) throws Exception {
        federation.removeRepository(name);
        federation.storeFederationConfig();
    }

    public NISDomain getRepository(String name) throws Exception {
        return (NISDomain)federation.getRepository(name);
    }
    
    public Collection<String> getRepositoryNames() throws Exception {
        return federation.getRepositoryNames();
    }
    
    public Collection<NISDomain> getRepositories() throws Exception {
        Collection<NISDomain> list = new ArrayList<NISDomain>();
        for (Repository repository : federation.getRepositories("NIS")) {
            list.add((NISDomain)repository);
        }
        return list;
    }

    public void createDatabase(NISDomain domain, PartitionConfig nisPartitionConfig) throws Exception {
        moduleClient.invoke(
                "createDatabase",
                new Object[] { domain, nisPartitionConfig },
                new String[] { NISDomain.class.getName(), PartitionConfig.class.getName() }
        );
    }

    public void removeDatabase(NISDomain domain) throws Exception {
        moduleClient.invoke(
                "removeDatabase",
                new Object[] { domain },
                new String[] { NISDomain.class.getName() }
        );
    }

    public void createPartitions(String name) throws Exception {
        federation.createPartitions(name);
        federation.startPartitions(name);
    }

    public void removePartitions(String name) throws Exception {
        federation.stopPartitions(name);
        federation.removePartitions(name);
    }

    public SynchronizationResult synchronizeNISMaps(String name, Collection<String> parameters) throws Exception {
        return (SynchronizationResult)moduleClient.invoke(
                "synchronizeNISMaps",
                new Object[] { name, parameters },
                new String[] { String.class.getName(), Collection.class.getName() }
        );
    }

    public void execute(Collection<String> commands) throws Exception {

        Iterator<String> iterator = commands.iterator();
        String command = iterator.next();

        if ("synchronize".equals(command)) {

            String repository = iterator.next();
            System.out.println("Synchronizing "+repository+"...");

            Collection<String> parameters = new ArrayList<String>();
            while (iterator.hasNext()) {
                String parameter = iterator.next();
                parameters.add(parameter);
            }

            SynchronizationResult result = synchronizeNISMaps(repository, parameters);
            System.out.println("Synchronization Result:");
            System.out.println(" - added     : "+result.getAddedEntries());
            System.out.println(" - modified  : "+result.getModifiedEntries());
            System.out.println(" - deleted   : "+result.getDeletedEntries());
            System.out.println(" - unchanged : "+result.getUnchangedEntries());
            System.out.println(" - failed    : "+result.getFailedEntries());
            System.out.println(" - total     : "+result.getTotalEntries());
            System.out.println(" - time      : "+result.getDuration()/1000.0+" s");

            System.out.println("Done.");

        } else {
            throw new Exception("Unknown command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.federation.NISFederationClient [OPTION]... <command> [arguments]...");
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
        System.out.println("  synchronize [<repository> [maps...]]  Synchronize NIS repository.");
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

        Getopt getopt = new Getopt("NISFederationClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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

            FederationClient federationClient = new FederationClient(client);
            NISFederationClient nisFederationClient = new NISFederationClient(federationClient);
            nisFederationClient.execute(commands);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
