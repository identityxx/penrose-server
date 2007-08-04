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
package org.safehaus.penrose.management;

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

    public final static String MBEAN_NAME = "Penrose:service=Penrose";

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

    public PenroseClient(String host, int port, String username, String password) throws Exception {
        this(PENROSE, host, port, username, password);
    }

	public PenroseClient(String type, String host, int port, String username, String password) throws Exception {
        this.type = type;
        this.host = host;
        this.port = port;

		this.username = username;
		this.password = password;
	}

    public PenroseClient(String type, String protocol, String host, int port, String username, String password) throws Exception {
        this.type = type;
        this.protocol = protocol;
        this.host = host;
        this.port = port;

        this.username = username;
        this.password = password;
    }

	public void connect() throws Exception {

        if (JBOSS.equals(type)) {

            String url = "jnp://"+host+":"+port;
            log.debug("Connecting to JBoss server at "+url);

            Hashtable<String,Object> parameters = new Hashtable<String,Object>();
            parameters.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
            parameters.put(Context.PROVIDER_URL, url);
            parameters.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
            parameters.put(Context.SECURITY_PRINCIPAL, username);
            parameters.put(Context.SECURITY_CREDENTIALS, password);

            context = new InitialContext(parameters);
            connection = (MBeanServerConnection)context.lookup("jmx/invoker/RMIAdaptor");

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

            Hashtable<String,Object> parameters = new Hashtable<String,Object>();
            parameters.put(JMXConnector.CREDENTIALS, credentials);

            connector = JMXConnectorFactory.connect(serviceURL, parameters);
            connection = connector.getMBeanServerConnection();
        }

		domain = connection.getDefaultDomain();
		name = new ObjectName(MBEAN_NAME);
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
		return connection.invoke(name, method, paramValues, paramClassNames);
	}

    public String getProductName() throws Exception {
        return (String)connection.getAttribute(name, "ProductName");
    }

    public String getProductVersion() throws Exception {
        return (String)connection.getAttribute(name, "ProductVersion");
    }

    public Collection<String> getPartitionNames() throws Exception {
        return (Collection<String>)connection.getAttribute(name, "PartitionNames");
    }

    public Collection<String> getServiceNames() throws Exception {
        return (Collection<String>)connection.getAttribute(name, "ServiceNames");
    }

    public void start() throws Exception {
        invoke("start",
                new Object[] { },
                new String[] { }
        );
    }

    public void stop() throws Exception {
        invoke("stop",
                new Object[] { },
                new String[] { }
        );
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

    public void restart() throws Exception {
        invoke("restart",
                new Object[] { },
                new String[] { }
        );
    }

    public void start(String serviceName) throws Exception {
        invoke("start",
                new Object[] { serviceName },
                new String[] { String.class.getName() }
        );
    }

    public void stop(String serviceName) throws Exception {
        invoke("stop",
                new Object[] { serviceName },
                new String[] { String.class.getName() }
        );
    }

    public String getStatus(String serviceName) throws Exception {
        return (String)invoke("getStatus",
                new Object[] { serviceName },
                new String[] { String.class.getName() }
        );
    }

    public Collection<String> listFiles(String directory) throws Exception {
        return (Collection<String>)invoke("listFiles",
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

    public Collection<String> getLoggerNames() throws Exception {
        return (Collection<String>)connection.getAttribute(name, "LoggerNames");
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

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.management.PenroseClient [OPTION]... <COMMAND>");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  version            get server version");
        System.out.println("  start <service>    start service");
        System.out.println("  stop <service>     stop service");
        System.out.println("  list               display service status");
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

        Iterator iterator = parameters.iterator();
        String command = (String)iterator.next();
        log.debug("Executing "+command);

        if ("version".equals(command)) {
            String version = client.getProductName()+" "+client.getProductVersion();
            System.out.println(version);

        } else if ("start".equals(command)) {
            if (iterator.hasNext()) {
                String serviceName = (String)iterator.next();
                client.start(serviceName);
            } else {
                client.start();
            }

        } else if ("stop".equals(command)) {
            if (iterator.hasNext()) {
                String serviceName = (String)iterator.next();
                client.stop(serviceName);
            } else {
                client.stop();
            }

        } else if ("restart".equals(command)) {
            client.restart();

        } else if ("reload".equals(command)) {
            client.reload();

        } else if ("store".equals(command)) {
            client.store();

        } else if ("rename".equals(command)) {
            String object = (String)iterator.next();
            if ("entry".equals(object)) {
                String oldDn = (String)iterator.next();
                String newDn = (String)iterator.next();
                log.debug("Renaming "+oldDn+" to "+newDn);
                client.renameEntryMapping(oldDn, newDn);
            }

        } else if ("list".equals(command)) {
            Collection<String> serviceNames = client.getServiceNames();
            for (String serviceName : serviceNames) {
                String status = client.getStatus(serviceName);

                StringBuilder padding = new StringBuilder();
                for (int j = 0; j < 20 - serviceName.length(); j++) padding.append(" ");

                System.out.println(serviceName + padding + "[" + status + "]");
            }

        } else if ("loggers".equals(command)) {
            Collection<String> loggerNames = client.getLoggerNames();
            for (String loggerName : loggerNames) {
                String l = client.getLoggerLevel(loggerName);

                System.out.println(loggerName + " [" + l + "]");
            }

        } else if ("logger".equals(command)) {
            String loggerName = (String)iterator.next();
            if (iterator.hasNext()) {
                String l = (String)iterator.next();
                client.setLoggerLevel(loggerName, "".equals(l) ? null : l);
            } else {
                String l = client.getLoggerLevel(loggerName);
                System.out.println(l);
            }
        }

        client.close();
    }
}
