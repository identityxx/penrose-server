package org.safehaus.penrose.federation;

import org.safehaus.penrose.client.PenroseClient;
import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;

import java.util.*;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * @author Endi Sukma Dewata
 */
public class NISFederationClient extends FederationRepositoryClient {

    public static Logger log = Logger.getLogger(NISFederationClient.class);

    public final static String CACHE_USERS           = "cache_users";
    public final static String CACHE_GROUPS          = "cache_groups";
    public final static String CACHE_CONNECTION_NAME = "Cache";

    public final static String CHANGE_USERS          = "change_users";
    public final static String CHANGE_GROUPS         = "change_groups";

    public NISFederationClient(FederationClient federationClient) throws Exception {
        super(federationClient, "NIS");
    }

    public SynchronizationResult synchronize(String name, Collection<String> parameters) throws Exception {
        return (SynchronizationResult)moduleClient.invoke(
                "synchronize",
                new Object[] { name, parameters },
                new String[] { String.class.getName(), Collection.class.getName() }
        );
    }

    public static SynchronizationResult synchronize(PenroseClient client, String partition, String name, Collection<String> parameters) throws Exception {
        FederationClient federationClient = new FederationClient(client, partition);
        NISFederationClient nisFederationClient = new NISFederationClient(federationClient);
        return nisFederationClient.synchronize(name, parameters);
    }

    public static void execute(PenroseClient client, Collection<String> commands) throws Exception {

        Iterator<String> iterator = commands.iterator();
        String command = iterator.next();

        if ("synchronize".equals(command)) {

            String partition = iterator.next();
            String repository = iterator.next();
            System.out.println("Synchronizing "+partition+" "+repository+"...");

            Collection<String> parameters = new ArrayList<String>();
            while (iterator.hasNext()) {
                String parameter = iterator.next();
                parameters.add(parameter);
            }

            SynchronizationResult result = synchronize(client, partition, repository, parameters);
            System.out.println("Result:");
            System.out.println(" - source    : "+result.getSourceEntries());
            System.out.println(" - target    : "+result.getTargetEntries());
            System.out.println(" - added     : "+result.getAddedEntries());
            System.out.println(" - modified  : "+result.getModifiedEntries());
            System.out.println(" - deleted   : "+result.getDeletedEntries());
            System.out.println(" - unchanged : "+result.getUnchangedEntries());
            System.out.println(" - failed    : "+result.getFailedEntries());
            System.out.println(" - time      : "+result.getDuration()/1000.0+" s");

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
        System.out.println("  synchronize <Federation domain> [<NIS domain> [NIS maps...]]  Synchronize NIS domain.");
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

            execute(client, commands);

            client.close();

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
