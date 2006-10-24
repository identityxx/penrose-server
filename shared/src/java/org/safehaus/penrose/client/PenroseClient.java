/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.client;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.*;
import java.io.File;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

public class PenroseClient {

    public static Logger log = Logger.getLogger(PenroseClient.class);

    public final static String NAME = "Penrose:service=Penrose";

    public final static String PENROSE = "PENROSE";
    public final static String JBOSS   = "JBOSS";

    public final static int DEFAULT_RMI_PORT            = 1099;
    public final static int DEFAULT_RMI_TRANSPORT_PORT  = 0;
    public final static int DEFAULT_HTTP_PORT           = 8112;
    public final static String DEFAULT_PROTOCOL         = "rmi";

	public String url;
    public String type;
    private String protocol      = DEFAULT_PROTOCOL;
    public String host;
    public int port              = DEFAULT_RMI_PORT;
    private int rmiTransportPort = DEFAULT_RMI_TRANSPORT_PORT;
    public String username;
    public String password;

    public Context context;
	public JMXConnector connector;

	public MBeanServerConnection connection;
	public String domain;
	public ObjectName name;

    ConnectionManagerClient connectionManagerClient;
    PartitionManagerClient partitionManagerClient;
    ServiceManagerClient serviceManagerClient;

    public PenroseClient(String host, int port, String username, String password) throws Exception {
        this(PENROSE, host, port, username, password);

        init();
    }

	public PenroseClient(String type, String host, int port, String username, String password) throws Exception {
        this.type = type;
        this.host = host;
        this.port = port;

		this.username = username;
		this.password = password;

        init();
	}

    public PenroseClient(String type, String protocol, String host, int port, String username, String password) throws Exception {
        this.type = type;
        this.protocol = protocol;
        this.host = host;
        this.port = port;

        this.username = username;
        this.password = password;

        init();
    }

    public void init() throws Exception {
        connectionManagerClient = new ConnectionManagerClient(this);
        partitionManagerClient = new PartitionManagerClient(this);
        serviceManagerClient = new ServiceManagerClient(this);
    }

    public void connect() throws Exception {

        if (JBOSS.equals(type)) {

            String url = "jnp://"+host+":"+port;
            log.debug("Connecting to JBoss server at "+url);

            Hashtable parameters = new Hashtable();
            parameters.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
            parameters.put(Context.PROVIDER_URL, url);
            parameters.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
            parameters.put(Context.SECURITY_PRINCIPAL, username);
            parameters.put(Context.SECURITY_CREDENTIALS, password);

            context = new InitialContext(parameters);
            setConnection((MBeanServerConnection)context.lookup("jmx/invoker/RMIAdaptor"));

        } else {

            String url = "service:jmx:"+protocol+"://"+host;
            if (rmiTransportPort != DEFAULT_RMI_TRANSPORT_PORT) url += ":"+rmiTransportPort;

            url += "/jndi/"+protocol+"://"+host;
            //if (port != DEFAULT_RMI_PORT)
            url += ":"+port;

            url += "/jmx";

            //String url = "service:jmx:"+protocol+"://"+host+(port == 0 ? "" : ":"+port);
            log.debug("Connecting to Penrose server at "+url);

            JMXServiceURL serviceURL = new JMXServiceURL(url);

            String[] credentials = new String[2];
            credentials[0] = username;
            credentials[1] = password;

            Hashtable parameters = new Hashtable();
            parameters.put(JMXConnector.CREDENTIALS, credentials);

            connector = JMXConnectorFactory.connect(serviceURL, parameters);
            setConnection(connector.getMBeanServerConnection());
        }

		domain = getConnection().getDefaultDomain();
		name = new ObjectName(NAME);
	}

    public void close() throws Exception {
        if (JBOSS.equals(type)) {
            context.close();
        } else {
            connector.close();
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getRmiTransportPort() {
        return rmiTransportPort;
    }

    public void setRmiTransportPort(int rmiTransportPort) {
        this.rmiTransportPort = rmiTransportPort;
    }

	public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {
		return getConnection().invoke(name, method, paramValues, paramClassNames);
	}

    public String getProductName() throws Exception {
        return (String)getConnection().getAttribute(name, "ProductName");
    }

    public String getProductVersion() throws Exception {
        return (String)getConnection().getAttribute(name, "ProductVersion");
    }

    public void reload() throws Exception {
        invoke("reload",
                new Object[] { },
                new String[] { }
        );
    }

    public void store() throws Exception {
        invoke("store",
                new Object[] { },
                new String[] { }
        );
    }

    public void renameEntryMapping(String oldDn, String newDn) throws Exception {
        invoke("renameEntryMapping",
                new Object[] { oldDn, newDn },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public Collection listFiles(String directory) throws Exception {
        return (Collection)invoke("listFiles",
                new Object[] { directory },
                new String[] { String.class.getName() }
        );
    }

    public byte[] download(String filename) throws Exception {
        return (byte[])invoke("download",
                new Object[] { filename },
                new String[] { String.class.getName() }
        );
    }

    public void upload(String filename, byte content[]) throws Exception {
        invoke("upload",
                new Object[] { filename, content },
                new String[] { String.class.getName(), "[B" }
        );
    }

    public Collection getLoggerNames() throws Exception {
        return (Collection)getConnection().getAttribute(name, "LoggerNames");
    }

    public String getLoggerLevel(String name) throws Exception {
        return (String)invoke("getLoggerLevel",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void setLoggerLevel(String name, String level) throws Exception {
        invoke("setLoggerLevel",
                new Object[] { name, level },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void execute(Collection parameters) throws Exception {

        Iterator iterator = parameters.iterator();
        String command = (String)iterator.next();
        log.debug("Executing "+command);

        if ("version".equals(command)) {
            String version = getProductName()+" "+getProductVersion();
            System.out.println(version);

        } else if ("service".equals(command)) {

            String name = (String)iterator.next();
            ServiceClient service = serviceManagerClient.getService(name);

            String action = (String)iterator.next();
            if ("start".equals(action)) {
                service.start();

            } else if ("stop".equals(action)) {
                service.stop();

            } else if ("restart".equals(action)) {
                service.restart();

            } else if ("info".equals(action)) {
                service.printInfo();

            } else {
                System.out.println("Invalid action: "+action);
            }

        } else if ("partition".equals(command)) {

            String name = (String)iterator.next();
            PartitionClient partition = partitionManagerClient.getPartitionClient(name);

            String action = (String)iterator.next();
            if ("start".equals(action)) {
                partitionManagerClient.start(name);

            } else if ("stop".equals(action)) {
                partitionManagerClient.stop(name);

            } else if ("restart".equals(action)) {
                partitionManagerClient.restart(name);

            } else if ("info".equals(action)) {
                partition.printInfo();

            } else {
                System.out.println("Invalid action: "+action);
            }

        } else if ("connection".equals(command)) {

            String name = (String)iterator.next();
            ConnectionClient connection = connectionManagerClient.getConnectionClient(name);

            String action = (String)iterator.next();
            if ("start".equals(action)) {
                connectionManagerClient.start(name);

            } else if ("stop".equals(action)) {
                connectionManagerClient.stop(name);

            } else if ("restart".equals(action)) {
                connectionManagerClient.restart(name);

            } else if ("info".equals(action)) {
                connection.printInfo();

            } else {
                System.out.println("Invalid action: "+action);
            }

        } else if ("restart".equals(command)) {
            serviceManagerClient.restart();

        } else if ("reload".equals(command)) {
            reload();

        } else if ("store".equals(command)) {
            store();

        } else if ("rename".equals(command)) {
            String object = (String)iterator.next();
            if ("entry".equals(object)) {
                String oldDn = (String)iterator.next();
                String newDn = (String)iterator.next();
                log.debug("Renaming "+oldDn+" to "+newDn);
                renameEntryMapping(oldDn, newDn);
            }

        } else if ("list".equals(command)) {

            String target = (String)iterator.next();
            if ("services".equals(target)) {
                serviceManagerClient.printServices();

            } else if ("partitions".equals(target)) {
                partitionManagerClient.printPartitions();

            } else if ("connections".equals(target)) {
                connectionManagerClient.printConnections();

            } else {
                System.out.println("Invalid command: "+command);
            }

        } else if ("loggers".equals(command)) {
            Collection loggerNames = getLoggerNames();
            for (Iterator i=loggerNames.iterator(); i.hasNext(); ) {
                String loggerName = (String)i.next();
                String l = getLoggerLevel(loggerName);

                System.out.println(loggerName+" ["+l +"]");
            }

        } else if ("logger".equals(command)) {
            String loggerName = (String)iterator.next();
            if (iterator.hasNext()) {
                String l = (String)iterator.next();
                setLoggerLevel(loggerName, "".equals(l) ? null : l);
            } else {
                String l = getLoggerLevel(loggerName);
                System.out.println(l);
            }

        } else {
            System.out.println("Invalid command: "+command);
        }
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.client.PenroseClient [OPTION]... <COMMAND>");
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
        System.out.println("  version                    get server version");
        System.out.println("  list services              display all services");
        System.out.println("  service <name> start       start service");
        System.out.println("  service <name> stop        stop service");
        System.out.println("  service <name> restart     restart service");
        System.out.println("  service <name> info        display service info");
        System.out.println("  list partitions            display all partitions");
        System.out.println("  partition <name> start     start partition");
        System.out.println("  partition <name> stop      stop partition");
        System.out.println("  partition <name> restart   restart partition");
        System.out.println("  partition <name> info      display partition info");
        System.out.println("  list connections           display all connections");
        System.out.println("  connection <name> start    start connection");
        System.out.println("  connection <name> stop     stop connection");
        System.out.println("  connection <name> restart  restart connection");
        System.out.println("  connection <name> info     display connection info");
    }

    public static void main(String args[]) throws Exception {

        Level level          = Level.WARN;
        String serverType    = PENROSE;
        String protocol      = DEFAULT_PROTOCOL;
        String hostname      = "localhost";
        int portNumber       = DEFAULT_RMI_PORT;
        int rmiTransportPort = DEFAULT_RMI_TRANSPORT_PORT;

        String bindDn = null;
        String bindPassword = null;

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("PenroseClient", args, "-:?dvt:h:p:r:P:D:w:", longopts);

        Collection parameters = new ArrayList();
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

        String homeDirectory = System.getProperty("penrose.home");

        //Logger rootLogger = Logger.getRootLogger();
        //rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        File log4jProperties = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.properties");
        File log4jXml = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"log4j.xml");

        if (level.equals(Level.DEBUG)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (level.equals(Level.INFO)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (log4jProperties.exists()) {
            PropertyConfigurator.configure(log4jProperties.getAbsolutePath());

        } else if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());

        } else {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);
        }

        PenroseClient client = new PenroseClient(serverType, protocol, hostname, portNumber, bindDn, bindPassword);
        client.setRmiTransportPort(rmiTransportPort);
        client.connect();

        client.execute(parameters);

        client.close();
    }

    public MBeanServerConnection getConnection() {
        return connection;
    }

    public void setConnection(MBeanServerConnection connection) {
        this.connection = connection;
    }

    public ServiceManagerClient getServiceManagerClient() {
        return serviceManagerClient;
    }

    public void setServiceManagerClient(ServiceManagerClient serviceManagerClient) {
        this.serviceManagerClient = serviceManagerClient;
    }

    public PartitionManagerClient getPartitionManagerClient() {
        return partitionManagerClient;
    }

    public void setPartitionManagerClient(PartitionManagerClient partitionManagerClient) {
        this.partitionManagerClient = partitionManagerClient;
    }

    public ConnectionManagerClient getConnectionManagerClient() {
        return connectionManagerClient;
    }

    public void setConnectionManagerClient(ConnectionManagerClient connectionManagerClient) {
        this.connectionManagerClient = connectionManagerClient;
    }
}
