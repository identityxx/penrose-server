/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.management;

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;

public class PenroseClient {

    public final static String MBEAN_NAME = "Penrose:type=Penrose";

	public String url;
    public String username;
    public String password;

	public JMXServiceURL address;
	public Map environment;
	public JMXConnector connector;
	public MBeanServerConnection connection;
	public String domain;
	public ObjectName name;
    public String host;
    public int port;

	public PenroseClient(String host, int port, String username, String password) throws Exception {
        this.host = host;
        this.port = port;
        this.url = "service:jmx:rmi:///jndi/rmi://"+host+(port == 0 ? "" : ":"+port)+"/jmx";
        //this.url = "service:jmx:rmi://"+host+(port == 0 ? "" : ":"+port);
		this.username = username;
		this.password = password;
	}

	public void connect() throws Exception {
		address = new JMXServiceURL(url);
        //address = new JMXServiceURL("rmi", host, port);

        String[] credentials = new String[2];
        credentials[0] = username;
        credentials[1] = password;

        environment = new HashMap();
        environment.put(JMXConnector.CREDENTIALS, credentials);

		connector = JMXConnectorFactory.connect(address, environment);
        //connector = JMXConnectorFactory.newJMXConnector(address, null);
        //connector.connect(environment);

		connection = connector.getMBeanServerConnection();
		domain = connection.getDefaultDomain();
		name = new ObjectName(MBEAN_NAME);
	}

    public void close() throws Exception {
        connector.close();
    }

	public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {
		Object obj = connection.invoke(name, method, paramValues, paramClassNames);
		return obj;
	}

    public Collection getFileNames(String directory) throws Exception {
        return (Collection)connection.invoke(name,
                "getFileNames",
                new Object[] { directory },
                new String[] { String.class.getName() }
        );
    }

    public String readConfigFile(String filename) throws Exception {
        return (String)connection.invoke(name,
                "readConfigFile",
                new Object[] { filename },
                new String[] { String.class.getName() }
        );
    }

    public void writeConfigFile(String filename, String content) throws Exception {
        connection.invoke(name,
                "writeConfigFile",
                new Object[] { filename, content },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public static void main(String args[]) throws Exception {
        PenroseClient client = new PenroseClient("localhost", 0, args[0], args[1]);
        client.connect();
        client.close();
    }
}
