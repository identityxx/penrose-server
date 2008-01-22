package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionConfig;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionService extends JMXService implements ConnectionServiceMBean {

    private Connection connection;

    public ConnectionService(Connection connection) throws Exception {
        super(connection, connection.getDescription());

        this.connection = connection;
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
        Partition partition = connection.getPartition();
        return ConnectionClient.getObjectName(partition.getName(), connection.getName());
    }

    public void start() throws Exception {
        Partition partition = connection.getPartition();
        log.debug("Starting connection "+partition.getName()+"/"+connection.getName()+"...");
        connection.init();
        log.debug("Connection started.");
    }

    public void stop() throws Exception {
        Partition partition = connection.getPartition();
        log.debug("Stopping connection "+partition.getName()+"/"+connection.getName()+"...");
        connection.destroy();
        log.debug("Connection stopped.");
    }

    public void restart() throws Exception {
        Partition partition = connection.getPartition();
        log.debug("Restarting connection "+partition.getName()+"/"+connection.getName()+"...");
        connection.destroy();
        connection.init();
        log.debug("Connection restarted.");
    }
}
