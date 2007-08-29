package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;

/**
 * @author Endi Sukma Dewata
 */
public class SourceClient implements SourceServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String partitionName;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public SourceClient(PenroseClient client, String partitionName, String name) throws Exception {
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

    public Long getCount() throws Exception {
        return (Long)connection.getAttribute(objectName, "Count");
    }

    public void createCache() throws Exception {
        invoke("createCache", new Object[] {}, new String[] {});
    }

    public void loadCache() throws Exception {
        invoke("loadCache", new Object[] {}, new String[] {});
    }

    public void cleanCache() throws Exception {
        invoke("cleanCache", new Object[] {}, new String[] {});
    }

    public void dropCache() throws Exception {
        invoke("dropCache", new Object[] {}, new String[] {});
    }

    public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {

        log.debug("Invoking method "+method+"() on "+name+".");

        return connection.invoke(
                objectName,
                method,
                paramValues,
                paramClassNames
        );
    }

    public static String getObjectName(String partitionName, String sourceName) {
        return "Penrose:type=source,partition="+partitionName+",name="+sourceName;
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
