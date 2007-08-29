package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.Penrose;

import javax.management.StandardMBean;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionService extends StandardMBean implements PartitionServiceMBean {

    private PenroseJMXService jmxService;
    private Partitions partitions;
    private String name;

    public PartitionService() throws Exception {
        super(PartitionServiceMBean.class);
    }

    public String getStatus() {
        PenroseServer penroseServer = jmxService.getServiceContext().getPenroseServer();
        Penrose penrose = penroseServer.getPenrose();

        return penrose.getPartitionStatus(name);
    }
    
    public void start() throws Exception {
        PenroseServer penroseServer = jmxService.getServiceContext().getPenroseServer();
        Penrose penrose = penroseServer.getPenrose();

        penrose.startPartition(name);
    }

    public void stop() throws Exception {
        PenroseServer penroseServer = jmxService.getServiceContext().getPenroseServer();
        Penrose penrose = penroseServer.getPenrose();

        penrose.stopPartition(name);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getConnectionNames() {
        Collection<String> list = new ArrayList<String>();
        Partition partition = partitions.getPartition(name);
        for (Connection connection : partition.getConnections()) {
            list.add(connection.getName());
        }
        return list;
    }

    public ConnectionService getConnectionService(String connectionName) throws Exception {
        Partition partition = partitions.getPartition(name);
        Connection connection = partition.getConnection(connectionName);
        if (connection == null) return null;

        return getConnectionService(connection);
    }

    public ConnectionService getConnectionService(Connection connection) throws Exception {

        ConnectionService connectionService = new ConnectionService();
        connectionService.setJmxService(jmxService);
        Partition partition = partitions.getPartition(name);
        connectionService.setPartition(partition);
        connectionService.setConnection(connection);

        return connectionService;
    }

    public Collection<ConnectionService> getConnectionServices() throws Exception {

        Collection<ConnectionService> list = new ArrayList<ConnectionService>();

        Partition partition = partitions.getPartition(name);
        for (Connection connection : partition.getConnections()) {
            list.add(getConnectionService(connection));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() {
        Collection<String> list = new ArrayList<String>();
        Partition partition = partitions.getPartition(name);
        for (Source source : partition.getSources()) {
            list.add(source.getName());
        }
        return list;
    }

    public SourceService getSourceService(String sourceName) throws Exception {
        Partition partition = partitions.getPartition(name);
        Source source = partition.getSource(sourceName);
        if (source == null) return null;

        return getSourceService(source);
    }

    public SourceService getSourceService(Source source) throws Exception {

        SourceService sourceService = new SourceService();
        sourceService.setJmxService(jmxService);
        Partition partition = partitions.getPartition(name);
        sourceService.setPartition(partition);
        sourceService.setSource(source);

        return sourceService;
    }
    
    public Collection<SourceService> getSourceServices() throws Exception {

        Collection<SourceService> list = new ArrayList<SourceService>();

        Partition partition = partitions.getPartition(name);
        for (Source source : partition.getSources()) {
            list.add(getSourceService(source));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() {
        Collection<String> list = new ArrayList<String>();
        Partition partition = partitions.getPartition(name);
        for (Module module : partition.getModules()) {
            list.add(module.getName());
        }
        return list;
    }

    public ModuleService getModuleService(String moduleName) throws Exception {
        Partition partition = partitions.getPartition(name);
        Module module = partition.getModule(moduleName);
        if (module == null) return null;

        return getModuleService(module);
    }

    public ModuleService getModuleService(Module module) throws Exception {

        Partition partition = partitions.getPartition(name);
        ModuleService moduleService = new ModuleService(partition, module);
        moduleService.setJmxService(jmxService);

        return moduleService;
    }

    public Collection<ModuleService> getModuleServices() throws Exception {

        Collection<ModuleService> list = new ArrayList<ModuleService>();

        Partition partition = partitions.getPartition(name);
        for (Module module : partition.getModules()) {
            list.add(getModuleService(module));
        }

        return list;
    }

    public String getObjectName() {
        return PartitionClient.getObjectName(name);
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        for (ConnectionService connectionService : getConnectionServices()) {
            connectionService.register();
        }

        for (SourceService sourceService : getSourceServices()) {
            sourceService.register();
        }

        for (ModuleService moduleService : getModuleServices()) {
            moduleService.register();
        }
    }

    public void unregister() throws Exception {

        for (ModuleService moduleService : getModuleServices()) {
            moduleService.unregister();
        }

        for (SourceService sourceService : getSourceServices()) {
            sourceService.unregister();
        }

        for (ConnectionService connectionService : getConnectionServices()) {
            connectionService.unregister();
        }

        jmxService.unregister(getObjectName());
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public Partitions getPartitions() {
        return partitions;
    }

    public void setPartitions(Partitions partitions) {
        this.partitions = partitions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
