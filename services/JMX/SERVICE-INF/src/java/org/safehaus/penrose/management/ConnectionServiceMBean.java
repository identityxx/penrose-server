package org.safehaus.penrose.management;

import org.safehaus.penrose.connection.ConnectionConfig;

/**
 * @author Endi Sukma Dewata
 */
public interface ConnectionServiceMBean {
    public ConnectionConfig getConnectionConfig() throws Exception;
}
