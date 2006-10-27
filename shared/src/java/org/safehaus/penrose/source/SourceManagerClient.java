package org.safehaus.penrose.source;

import org.safehaus.penrose.client.PenroseClient;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class SourceManagerClient implements SourceManagerMBean {

    public final static String NAME = "Penrose:name=SourceManager";

    PenroseClient client;
    ObjectName objectName;

    public SourceManagerClient(PenroseClient client) throws Exception {
        this.client = client;
        objectName = new ObjectName(SourceManagerClient.NAME);
    }

    public void start(String partitionName, String sourceName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "start",
                new Object[] { partitionName, sourceName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void stop(String partitionName, String sourceName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "stop",
                new Object[] { partitionName, sourceName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void restart(String partitionName, String sourceName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "restart",
                new Object[] { partitionName, sourceName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public String getStatus(String partitionName, String sourceName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "getStatus",
                new Object[] { partitionName, sourceName },
                new String[] { String.class.getName(), String.class.getName() }
        );
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

    public Collection getSourceNames(String partitionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.invoke(
                objectName,
                "getSourceNames",
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

    public SourceConfig getSourceConfig(String partitionName, String sourceName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (SourceConfig)connection.invoke(
                objectName,
                "getSourceConfig",
                new Object[] { partitionName, sourceName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public SourceClient getSourceClient(String partitionName, String sourceName) throws Exception {
        return new SourceClient(client, partitionName, sourceName);
    }
}
