package org.safehaus.penrose.client;

import org.safehaus.penrose.partition.PartitionManagerMBean;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.util.Formatter;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class PartitionManagerClient implements PartitionManagerMBean {

    PenroseClient client;
    ObjectName objectName;

    public PartitionManagerClient(PenroseClient client) throws Exception {
        this.client = client;
        objectName = new ObjectName(PartitionManagerMBean.NAME);
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

    public PartitionConfig getPartitionConfig(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (PartitionConfig)connection.invoke(
                objectName,
                "getPartitionConfig",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public String getStatus(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "getStatus",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void start(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "start",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void stop(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "stop",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void restart(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "restart",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public PartitionClient getPartitionClient(String name) throws Exception {
        return new PartitionClient(client, name);
    }

    public void printPartitions() throws Exception {
        System.out.print(Formatter.rightPad("PARTITION", 15)+" ");
        System.out.println(Formatter.rightPad("STATUS", 10));

        System.out.print(Formatter.repeat("-", 15)+" ");
        System.out.println(Formatter.repeat("-", 10));

        for (Iterator i=getPartitionNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String status = getStatus(name);

            System.out.print(Formatter.rightPad(name, 15)+" ");
            System.out.println(Formatter.rightPad(status, 10));
        }
    }
}
