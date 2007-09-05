package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.module.ModuleConfig;

import javax.management.*;

/**
 * @author Endi Sukma Dewata
 */
public class ModuleClient implements ModuleServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String partitionName;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public ModuleClient(PenroseClient client, String partitionName, String name) throws Exception {
        this.client = client;
        this.partitionName = partitionName;
        this.name = name;

        connection = client.getConnection();
        objectName = ObjectName.getInstance(getObjectName(partitionName, name));
    }

    public ModuleConfig getModuleConfig() throws Exception {
        return (ModuleConfig)connection.getAttribute(objectName, "ModuleConfig");
    }

    public static String getObjectName(String partitionName, String sourceName) {
        return "Penrose:type=module,partition="+partitionName+",name="+sourceName;
    }

    public MBeanAttributeInfo[] getAttributes() throws Exception {
        MBeanInfo info = connection.getMBeanInfo(objectName);
        return info.getAttributes();
    }

    public MBeanOperationInfo[] getOperations() throws Exception {
        MBeanInfo info = connection.getMBeanInfo(objectName);
        return info.getOperations();
    }

    public Object invoke(String method, Object[] paramValues, String[] paramTypes) throws Exception {

        log.debug("Invoking method "+method+"() on "+name+".");

        return connection.invoke(
                objectName,
                method,
                paramValues,
                paramTypes
        );
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
