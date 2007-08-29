package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionClient implements ConnectionServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String partitionName;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public ConnectionClient(PenroseClient client, String partitionName, String name) throws Exception {
        this.client = client;
        this.partitionName = partitionName;
        this.name = name;

        connection = client.getConnection();
        objectName = ObjectName.getInstance(getObjectName(partitionName, name));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static String getObjectName(String partitionName, String sourceName) {
        return "Penrose:type=connection,partition="+partitionName+",name="+sourceName;
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
}
