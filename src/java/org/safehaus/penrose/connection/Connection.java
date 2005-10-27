package org.safehaus.penrose.connection;

import org.safehaus.penrose.connection.Adapter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.ConnectionConfig;
import org.safehaus.penrose.mapping.SourceDefinition;
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

    public int bind(SourceDefinition sourceDefinition, AttributeValues values, String password) throws Exception {
        return adapter.bind(sourceDefinition, values, password);
    }

    public SearchResults search(SourceDefinition sourceDefinition, Filter filter, long sizeLimit) throws Exception {
        return adapter.search(sourceDefinition, filter, sizeLimit);
    }

    public SearchResults load(SourceDefinition sourceDefinition, Filter filter, long sizeLimit) throws Exception {
        return adapter.load(sourceDefinition, filter, sizeLimit);
    }

    public int add(SourceDefinition sourceDefinition, AttributeValues values) throws Exception {
        return adapter.add(sourceDefinition, values);
    }

    public int modify(SourceDefinition sourceDefinition, AttributeValues oldValues, AttributeValues newValues) throws Exception {
        return adapter.modify(sourceDefinition, oldValues, newValues);
    }

    public int delete(SourceDefinition sourceDefinition, AttributeValues values) throws Exception {
        return adapter.delete(sourceDefinition, values);
    }
}
