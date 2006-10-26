package org.safehaus.penrose.connection;

import org.safehaus.penrose.connection.ConnectionManagerMBean;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.client.PenroseClient;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManagerClient implements ConnectionManagerMBean {

    public final static String NAME = "Penrose:name=ConnectionManager";

    PenroseClient client;
    ObjectName objectName;

    public ConnectionManagerClient(PenroseClient client) throws Exception {
        this.client = client;
        objectName = new ObjectName(ConnectionManagerClient.NAME);
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public Collection getPartitionNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.getAttribute(objectName, "PartitionNames");

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
    }

    public Collection getConnectionNames(String partitionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.invoke(
                objectName,
                "getConnectionNames",
                new Object[] { partitionName },
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

    public ConnectionConfig getConnectionConfig(String partitionName, String connectionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (ConnectionConfig)connection.invoke(
                objectName,
                "getConnectionConfig",
                new Object[] { partitionName, connectionName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public String getStatus(String partitionName, String connectionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "getStatus",
                new Object[] { partitionName, connectionName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void start(String partitionName, String connectionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "start",
                new Object[] { partitionName, connectionName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void stop(String partitionName, String connectionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "stop",
                new Object[] { partitionName, connectionName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void restart(String partitionName, String connectionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "restart",
                new Object[] { partitionName, connectionName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public ConnectionClient getConnectionClient(String partitionName, String connectionName) throws Exception {
        return new ConnectionClient(client, partitionName, connectionName);
    }
}
