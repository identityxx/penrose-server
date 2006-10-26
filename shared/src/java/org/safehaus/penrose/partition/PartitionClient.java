package org.safehaus.penrose.partition;

import org.safehaus.penrose.partition.PartitionMBean;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.client.PenroseClient;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;

/**
 * @author Endi S. Dewata
 */
public class PartitionClient implements PartitionMBean {

    PenroseClient client;
    ObjectName objectName;

    public PartitionClient(PenroseClient client, String name) throws Exception {
        this.client = client;
        this.objectName = new ObjectName("Penrose Partitions:name="+name+",type=Partition");
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public String getName() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.getAttribute(objectName, "Name");
    }

    public PartitionConfig getPartitionConfig() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (PartitionConfig)connection.getAttribute(objectName, "PartitionConfig");
    }

    public String getStatus() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.getAttribute(objectName, "Status");
    }

    public void start() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "start",
                new Object[] { },
                new String[] { }
        );
    }

    public void stop() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "stop",
                new Object[] { },
                new String[] { }
        );
    }

    public void restart() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "restart",
                new Object[] { },
                new String[] { }
        );
    }
}
