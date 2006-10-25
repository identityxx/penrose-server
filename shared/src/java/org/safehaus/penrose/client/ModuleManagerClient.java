package org.safehaus.penrose.client;

import org.safehaus.penrose.module.ModuleManagerMBean;
import org.safehaus.penrose.module.ModuleConfig;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.Collection;
import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class ModuleManagerClient implements ModuleManagerMBean {

    public final static String NAME = "Penrose:name=ModuleManager";

    PenroseClient client;
    ObjectName objectName;

    public ModuleManagerClient(PenroseClient client) throws Exception {
        this.client = client;
        objectName = new ObjectName(ConnectionManagerClient.NAME);
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

    public String getStatus(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "getStatus",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public Collection getModuleNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.getAttribute(objectName, "ModuleNames");

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
    }

    public ModuleConfig getModuleConfig(String name) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (ModuleConfig)connection.invoke(
                objectName,
                "getModuleConfig",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }
}
