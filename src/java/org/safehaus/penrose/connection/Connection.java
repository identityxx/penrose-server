package org.safehaus.penrose.connection;

import org.safehaus.penrose.connection.Adapter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.ConnectionConfig;
import org.safehaus.penrose.filter.Filter;

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

    public int bind(Source source, AttributeValues values, String password) throws Exception {
        return adapter.bind(source, values, password);
    }

    public SearchResults search(Source source, Filter filter, long sizeLimit) throws Exception {
        return adapter.search(source, filter, sizeLimit);
    }

    public int add(Source source, AttributeValues values) throws Exception {
        return adapter.add(source, values);
    }

    public int modify(Source source, AttributeValues oldValues, AttributeValues newValues) throws Exception {
        return adapter.modify(source, oldValues, newValues);
    }

    public int delete(Source source, AttributeValues values) throws Exception {
        return adapter.delete(source, values);
    }
}
