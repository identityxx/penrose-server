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
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.service.ServiceManagerClient;
import org.safehaus.penrose.connection.ConnectionManagerClient;
import org.safehaus.penrose.module.ModuleManagerClient;
import org.safehaus.penrose.source.SourceManagerClient;

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.*;

public class PenroseClient {

    public static Logger log = Logger.getLogger(PenroseClient.class);

    public final static String NAME = "Penrose:service=Penrose";

    public final static String PENROSE = "PENROSE";
    public final static String SOAP    = "SOAP";
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
    SourceManagerClient sourceManagerClient;
    ModuleManagerClient moduleManagerClient;
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
        setConnectionManagerClient(new ConnectionManagerClient(this));
        setSourceManagerClient(new SourceManagerClient(this));
        setModuleManagerClient(new ModuleManagerClient(this));
        setPartitionManagerClient(new PartitionManagerClient(this));
        setServiceManagerClient(new ServiceManagerClient(this));
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
            connection = (MBeanServerConnection)context.lookup("jmx/invoker/RMIAdaptor");

        } else if (SOAP.equals(type)) {
            JMXServiceURL serviceURL = new JMXServiceURL("soap", host, port, "/jmxconnector");
            log.debug("Connecting to SOAP Connector at "+serviceURL);

            String[] credentials = new String[2];
            credentials[0] = username;
            credentials[1] = password;

            Hashtable parameters = new Hashtable();
            parameters.put(JMXConnector.CREDENTIALS, credentials);

            connector = JMXConnectorFactory.connect(serviceURL, parameters);
            connection = connector.getMBeanServerConnection();

        } else {
            String url = "service:jmx:"+protocol+"://"+host;
            if (rmiTransportPort != DEFAULT_RMI_TRANSPORT_PORT) url += ":"+rmiTransportPort;

            url += "/jndi/"+protocol+"://"+host;
            //if (port != DEFAULT_RMI_PORT)
            url += ":"+port;

            url += "/jmx";

            //String url = "service:jmx:"+protocol+"://"+host+(port == 0 ? "" : ":"+port);

            JMXServiceURL serviceURL = new JMXServiceURL(url);
            //JMXServiceURL serviceURL = new JMXServiceURL("rmi", host, port, null);
            log.debug("Connecting to RMI Connector at "+serviceURL);

            String[] credentials = new String[2];
            credentials[0] = username;
            credentials[1] = password;

            Hashtable parameters = new Hashtable();
            parameters.put(JMXConnector.CREDENTIALS, credentials);

            connector = JMXConnectorFactory.connect(serviceURL, parameters);
            connection = connector.getMBeanServerConnection();
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
        Object object = invoke("listFiles",
                new Object[] { directory },
                new String[] { String.class.getName() }
        );

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
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
        Object object = getConnection().getAttribute(name, "LoggerNames");

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
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

    public ModuleManagerClient getModuleManagerClient() {
        return moduleManagerClient;
    }

    public void setModuleManagerClient(ModuleManagerClient moduleManagerClient) {
        this.moduleManagerClient = moduleManagerClient;
    }

    public SourceManagerClient getSourceManagerClient() {
        return sourceManagerClient;
    }

    public void setSourceManagerClient(SourceManagerClient sourceManagerClient) {
        this.sourceManagerClient = sourceManagerClient;
    }
}
