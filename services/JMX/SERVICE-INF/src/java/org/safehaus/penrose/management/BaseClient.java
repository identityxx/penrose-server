package org.safehaus.penrose.management;

import org.apache.log4j.Logger;

import javax.management.*;

/**
 * @author Endi Sukma Dewata
 */
public class BaseClient {

    public Logger log = Logger.getLogger(getClass());

    protected PenroseClient client;
    protected String name;

    protected MBeanServerConnection connection;
    protected ObjectName objectName;

    public BaseClient(PenroseClient client, String name, String objectName) throws Exception {
        this(client, name, ObjectName.getInstance(objectName));
    }

    public BaseClient(PenroseClient client, String name, ObjectName objectName) throws Exception {
        this.client = client;
        this.name = name;

        this.connection = client.getConnection();
        this.objectName = objectName;
    }

    public boolean exists() throws Exception {
        try {
            connection.getMBeanInfo(objectName);
            return true;

        } catch (Exception e) {
            return false;
        }
    }

    public Object invoke(String method) throws Exception {

        log.debug("Invoke method "+method+"() on "+objectName+".");

        return connection.invoke(objectName, method, null, null);
    }
    
    public Object invoke(String method, Object[] paramValues, String[] paramClassNames) throws Exception {

        log.debug("Invoking method "+method+"() on "+ objectName +".");

        return connection.invoke(objectName, method, paramValues, paramClassNames);
    }

    public Object getAttribute(String attributeName) throws Exception {

        log.debug("Getting attribute "+ attributeName +" from "+name+".");

        return connection.getAttribute(objectName, attributeName);
    }

    public void setAttribute(String attributeName, Object value) throws Exception {

        log.debug("Setting attribute "+ attributeName +" from "+name+".");

        Attribute attribute = new Attribute(attributeName, value);
        connection.setAttribute(objectName, attribute);
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MBeanServerConnection getConnection() {
        return connection;
    }

    public void setConnection(MBeanServerConnection connection) {
        this.connection = connection;
    }

    public ObjectName getObjectName() {
        return objectName;
    }

    public void setObjectName(ObjectName objectName) {
        this.objectName = objectName;
    }

    public MBeanAttributeInfo[] getAttributes() throws Exception {
        MBeanInfo mbeanInfo = connection.getMBeanInfo(objectName);
        return mbeanInfo.getAttributes();
    }

    public MBeanOperationInfo[] getOperations() throws Exception {
        MBeanInfo mbeanInfo = connection.getMBeanInfo(objectName);
        return mbeanInfo.getOperations();
    }
}
