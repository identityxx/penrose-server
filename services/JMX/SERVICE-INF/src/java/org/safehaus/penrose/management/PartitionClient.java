package org.safehaus.penrose.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.PartitionConfig;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionClient implements PartitionServiceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseClient client;
    private String name;

    MBeanServerConnection connection;
    ObjectName objectName;

    public PartitionClient(PenroseClient client, String name) throws Exception {
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

    public PartitionConfig getPartitionConfig() throws Exception {
        return (PartitionConfig)connection.getAttribute(objectName, "PartitionConfig");
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getConnectionNames() throws Exception {
        return (Collection<String>)connection.getAttribute(objectName, "ConnectionNames");
    }

    public ConnectionClient getConnectionClient(String connectionName) throws Exception {
        return new ConnectionClient(client, name, connectionName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() throws Exception {
        return (Collection<String>)connection.getAttribute(objectName, "SourceNames");
    }

    public SourceClient getSourceClient(String sourceName) throws Exception {
        return new SourceClient(client, name, sourceName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() throws Exception {
        return (Collection<String>)connection.getAttribute(objectName, "ModuleNames");
    }

    public ModuleClient getModuleClient(String moduleName) throws Exception {
        return new ModuleClient(client, name, moduleName);
    }

    public static String getObjectName(String name) {
        return "Penrose:type=partition,name="+name;
    }

    public PenroseClient getClient() {
        return client;
    }

    public void setClient(PenroseClient client) {
        this.client = client;
    }
}
