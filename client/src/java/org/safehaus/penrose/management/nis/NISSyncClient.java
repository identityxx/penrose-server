package org.safehaus.penrose.management.nis;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.management.*;
import org.safehaus.penrose.management.module.ModuleClient;
import org.safehaus.penrose.management.partition.PartitionClient;
import org.safehaus.penrose.management.partition.PartitionManagerClient;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.DNBuilder;
import org.safehaus.penrose.nis.NIS;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

public class NISSyncClient {

    public static Logger log = Logger.getLogger(NISSyncClient.class);

    PenroseClient client;

    public NISSyncClient(
            String serverType,
            String protocol,
            String hostname,
            int port,
            String bindDn,
            String bindPassword
    ) throws Exception {

        client = new PenroseClient(
                serverType,
                protocol,
                hostname,
                port,
                bindDn,
                bindPassword
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

    public void execute(Collection<String> parameters) throws Exception {

        Iterator<String> iterator = parameters.iterator();
        String command = iterator.next();

        if ("synchronize".equals(command)) {
            String domain = iterator.next();

            if (iterator.hasNext()) {
                String map = iterator.next();
                synchronize(domain, map);

            } else {
                synchronize(domain);
            }
            
        } else {
            throw new Exception("Unknown command: "+command);
        }
    }

    public void synchronize(String domain) throws Exception {
        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(domain+"_nis");
        DN suffix = partitionClient.getSuffixes().iterator().next();
        ModuleClient moduleClient = partitionClient.getModuleClient("NISLDAPSyncModule");

        for (String map : NIS.mapLabels.keySet()) {
            synchronize(moduleClient, map, suffix);
        }
    }

    public void synchronize(String domain, String map) throws Exception {
        PartitionManagerClient partitionManagerClient = client.getPartitionManagerClient();
        PartitionClient partitionClient = partitionManagerClient.getPartitionClient(domain+"_nis");
        ModuleClient moduleClient = partitionClient.getModuleClient("NISLDAPSyncModule");
        DN suffix = partitionClient.getSuffixes().iterator().next();

        synchronize(moduleClient, map, suffix);
    }

    public void synchronize(ModuleClient moduleClient, String map, DN suffix) throws Exception {
        String label = NIS.mapLabels.get(map);

        if (label == null) {
            System.out.println("Unknown map: "+map);
            return;
        }

        DNBuilder db = new DNBuilder();
        db.append("ou="+label);
        db.append(suffix);
        DN dn = db.toDn();

        System.out.println("Synchronizing "+map+"...");

        Object returnValue = moduleClient.invoke(
                "synchronize",
                new Object[] { dn },
                new String[] { DN.class.getName() }
        );

        System.out.println("Return value: "+returnValue);
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.management.nis.NISSyncClient [OPTION]... <command> [arguments]...");
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
        System.out.println("  synchronize <domain> [map]    Synchronize NIS domain into LDAP.");
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

        Getopt getopt = new Getopt("Client", args, "-:?dvt:h:p:r:P:D:w:", longopts);

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
            NISSyncClient client = new NISSyncClient(
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

        } catch (SecurityException e) {
            log.error(e.getMessage());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
