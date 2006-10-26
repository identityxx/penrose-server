package org.safehaus.penrose.module;

import org.safehaus.penrose.client.PenroseClient;

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
        objectName = new ObjectName(ModuleManagerClient.NAME);
    }

    public void start(String partitionName, String moduleName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "start",
                new Object[] { partitionName, moduleName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void stop(String partitionName, String moduleName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "stop",
                new Object[] { partitionName, moduleName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public void restart(String partitionName, String moduleName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        connection.invoke(
                objectName,
                "restart",
                new Object[] { partitionName, moduleName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public String getStatus(String partitionName, String moduleName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (String)connection.invoke(
                objectName,
                "getStatus",
                new Object[] { partitionName, moduleName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public Collection getPartitionNames() throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.getAttribute(objectName, "PartitionNames");

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
    }

    public Collection getModuleNames(String partitionName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        Object object = connection.invoke(
                objectName,
                "getModuleNames",
                new Object[] { partitionName },
                new String[] { String.class.getName() }
        );

        if (object instanceof Object[]) {
            return Arrays.asList((Object[])object);

        } else if (object instanceof Collection) {
            return (Collection)object;

        } else {
            return null;
        }
    }

    public ModuleConfig getModuleConfig(String partitionName, String moduleName) throws Exception {
        MBeanServerConnection connection = client.getConnection();
        return (ModuleConfig)connection.invoke(
                objectName,
                "getModuleConfig",
                new Object[] { partitionName, moduleName },
                new String[] { String.class.getName(), String.class.getName() }
        );
    }

    public ModuleClient getModuleClient(String partitionName, String moduleName) throws Exception {
        return new ModuleClient(client, partitionName, moduleName);
    }
}
