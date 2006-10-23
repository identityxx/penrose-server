package org.safehaus.penrose.client;

import org.safehaus.penrose.service.ServiceMBean;
import org.safehaus.penrose.service.ServiceConfig;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ServiceClient implements ServiceMBean {

    PenroseClient client;
    ObjectName objectName;

    public ServiceClient(PenroseClient client, String name) throws Exception {
        this.client = client;
        this.objectName = new ObjectName("Penrose Services:name="+name+",type=Service");
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

    public String getServiceClass() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.getAttribute(objectName, "ServiceClass");
    }

    public String getDescription() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.getAttribute(objectName, "Description");
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

    public ServiceConfig getServiceConfig() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (ServiceConfig)connection.getAttribute(objectName, "ServiceConfig");
    }

    public Collection getParameterNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (Collection)connection.getAttribute(objectName, "ParameterNames");
    }

    public String getParameter(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "getParameter",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void setParameter(String name, String value) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "setParameter",
                new Object[] { name, value },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public String removeParameter(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "removeParameter",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void printInfo() throws Exception {
        System.out.println("Service     : "+getName());
        System.out.println("Class       : "+getServiceClass());

        String description = getDescription();
        System.out.println("Description : "+(description == null ? "" : description));

        System.out.println("Status      : "+getStatus());
        System.out.println();

        System.out.println("Parameters  : ");
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String paramName = (String)i.next();
            String value = getParameter(paramName);
            System.out.println(" - "+paramName +": "+value);
        }
    }
}
