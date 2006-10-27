package org.safehaus.penrose.source;

import org.safehaus.penrose.client.PenroseClient;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class SourceClient implements SourceMBean {

    PenroseClient client;
    ObjectName objectName;

    public SourceClient(PenroseClient client, String partitionName, String sourceName) throws Exception {
        this.client = client;
        this.objectName = new ObjectName("Penrose Sources:name="+sourceName +",partition="+partitionName+",type=Source");
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

    public SourceConfig getSourceConfig() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (SourceConfig)connection.getAttribute(objectName, "SourceConfig");
    }

    public Collection getParameterNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.getAttribute(objectName, "ParameterNames");

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
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
}
