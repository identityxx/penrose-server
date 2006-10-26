package org.safehaus.penrose.service;

import org.safehaus.penrose.service.ServiceManagerMBean;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.service.ServiceClient;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.client.PenroseClient;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class ServiceManagerClient implements ServiceManagerMBean {

    PenroseClient client;
    ObjectName objectName;

    public ServiceManagerClient(PenroseClient client) throws Exception {
        this.client = client;
        objectName = new ObjectName(ServiceManagerMBean.NAME);
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }

    public Collection getServiceNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.getAttribute(objectName, "ServiceNames");

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
    }

    public ServiceConfig getServiceConfig(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (ServiceConfig)connection.invoke(
                objectName,
                "getServiceConfig",
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

    public ServiceClient getService(String name) throws Exception {
        return new ServiceClient(client, name);
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

    public void start(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "start",
                new Object[] { name },
                new String[] { String.class.getName() }
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

    public void stop(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "stop",
                new Object[] { name },
                new String[] { String.class.getName() }
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

    public void restart(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "restart",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }
}
