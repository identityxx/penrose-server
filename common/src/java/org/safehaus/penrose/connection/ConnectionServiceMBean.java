package org.safehaus.penrose.connection;

import org.safehaus.penrose.connection.ConnectionConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface ConnectionServiceMBean {

    public final static String STARTED = "STARTED";
    public final static String STOPPED = "STOPPED";

    public String getStatus() throws Exception;

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;

    public ConnectionConfig getConnectionConfig() throws Exception;
    public void setConnectionConfig(ConnectionConfig connectionConfig) throws Exception;
    public String getAdapterName() throws Exception;
}
