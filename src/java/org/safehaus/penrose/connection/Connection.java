package org.safehaus.penrose.connection;

import org.safehaus.penrose.connection.Adapter;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Connection {

    private ConnectionConfig connectionConfig;
    private Adapter adapter;

    public void init(ConnectionConfig connectionConfig, Adapter adapter) {
        this.connectionConfig = connectionConfig;
        this.adapter = adapter;
    }

    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public Adapter getAdapter() {
        return adapter;
    }

    public void setAdapter(Adapter adapter) {
        this.adapter = adapter;
    }

    public String getParameter(String name) {
        return connectionConfig.getParameter(name);
    }

    public Collection getParameterNames() {
        return connectionConfig.getParameterNames();
    }

    public String getConnectionName() {
        return connectionConfig.getConnectionName();
    }
}
