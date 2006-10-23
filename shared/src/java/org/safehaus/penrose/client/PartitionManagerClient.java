package org.safehaus.penrose.client;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PartitionManagerClient {

    public final static String NAME = "Penrose:name=PartitionManager";

    PenroseClient client;
    ObjectName objectName;

    public PartitionManagerClient(PenroseClient client) throws Exception {
        this.client = client;
        objectName = new ObjectName(NAME);
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public Collection getPartitionNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (Collection)connection.getAttribute(objectName, "PartitionNames");
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


    public PartitionClient getPartition(String name) throws Exception {
        return new PartitionClient(client, name);
    }

    public void printPartitions() throws Exception {
        for (Iterator i=getPartitionNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String status = getStatus(name);

            StringBuffer padding = new StringBuffer();
            for (int j=0; j<20-name.length(); j++) padding.append(" ");

            System.out.println(name +padding+"["+status+"]");
        }
    }
}
