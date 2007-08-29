package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceClient implements ServiceServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public ServiceClient(PenroseClient client, String name) throws Exception {
        this.client = client;
        this.name = name;

        connection = client.getConnection();
        objectName = ObjectName.getInstance(getObjectName(name));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() throws Exception {
        return (String)connection.getAttribute(objectName, "Status");
    }

    public void start() throws Exception {
        connection.invoke(objectName, "start", new Object[] {}, new String[] {});
    }

    public void stop() throws Exception {
        connection.invoke(objectName, "stop", new Object[] {}, new String[] {});
    }

    public static String getObjectName(String name) {
        return "Penrose:type=service,name="+name;
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }
}
