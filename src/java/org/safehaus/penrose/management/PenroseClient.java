/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.management;

import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Map;

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

	public PenroseClient(String url, String username, String password) throws Exception {
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public void connect() throws Exception {
		address = new JMXServiceURL(url);
		environment = null;
		connector = JMXConnectorFactory.connect(address, environment);
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
}
