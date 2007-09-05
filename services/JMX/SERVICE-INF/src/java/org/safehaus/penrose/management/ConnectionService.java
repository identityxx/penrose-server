package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionConfig;

import javax.management.StandardMBean;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionService extends StandardMBean implements ConnectionServiceMBean {

    private PenroseJMXService jmxService;
    private Partition partition;
    private Connection connection;

    public ConnectionService() throws Exception {
        super(ConnectionServiceMBean.class);
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConnectionConfig getConnectionConfig() throws Exception {
        return connection.getConnectionConfig();
    }

    public String getObjectName() {
        return SourceClient.getObjectName(partition.getName(), connection.getName());
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);
    }

    public void unregister() throws Exception {
        jmxService.unregister(getObjectName());
    }
}
