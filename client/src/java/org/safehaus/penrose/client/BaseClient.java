package org.safehaus.penrose.client;

import org.apache.log4j.Logger;
import org.safehaus.penrose.util.ClassUtil;
import org.safehaus.penrose.client.PenroseClient;

import javax.management.*;

/**
 * @author Endi Sukma Dewata
 */
public class BaseClient {

    public Logger log = Logger.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

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
        return invoke(method, null, null);
    }
    
    public Object invoke(String method, Object[] paramValues, String[] paramTypes) throws Exception {

        if (debug) {
            String signature = ClassUtil.getSignature(method, paramTypes);
            log.debug("Invoking method "+signature+" on "+objectName+".");
        }

        Object object = connection.invoke(objectName, method, paramValues, paramTypes);
        if (debug) log.debug("Invoke method completed.");

        return object;
    }

    public Object getAttribute(String attributeName) throws Exception {

        if (debug) log.debug("Getting attribute "+ attributeName +" from "+objectName+".");

        Object object = connection.getAttribute(objectName, attributeName);
        if (debug) log.debug("Get attribute completed.");

        return object;
    }

    public void setAttribute(String attributeName, Object value) throws Exception {

        if (debug) log.debug("Setting attribute "+ attributeName +" from "+objectName+".");

        Attribute attribute = new Attribute(attributeName, value);
        connection.setAttribute(objectName, attribute);

        if (debug) log.debug("Set attribute completed.");
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
