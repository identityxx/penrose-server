package org.safehaus.penrose.connector;

import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.ConnectionConfig;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Connection {

    private ConnectionConfig connectionConfig;
    private Adapter adapter;

    public void init(ConnectionConfig connectionConfig, AdapterConfig adapterConfig) throws Exception {
        this.connectionConfig = connectionConfig;

        String adapterClass = adapterConfig.getAdapterClass();
        Class clazz = Class.forName(adapterClass);
        adapter = (Adapter)clazz.newInstance();
        
        adapter.init(adapterConfig, this);
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

    public String removeParameter(String name) {
        return connectionConfig.removeParameter(name);
    }

    public String getConnectionName() {
        return connectionConfig.getName();
    }

    public int bind(SourceConfig sourceConfig, AttributeValues values, String password) throws Exception {
        return adapter.bind(sourceConfig, values, password);
    }

    public PenroseSearchResults search(SourceConfig sourceConfig, Filter filter, long sizeLimit) throws Exception {
        return adapter.search(sourceConfig, filter, sizeLimit);
    }

    public PenroseSearchResults load(SourceConfig sourceConfig, Filter filter, long sizeLimit) throws Exception {
        return adapter.load(sourceConfig, filter, sizeLimit);
    }

    public int add(SourceConfig sourceConfig, AttributeValues values) throws Exception {
        return adapter.add(sourceConfig, values);
    }

    public int modify(SourceConfig sourceConfig, AttributeValues oldValues, AttributeValues newValues) throws Exception {
        return adapter.modify(sourceConfig, oldValues, newValues);
    }

    public int delete(SourceConfig sourceConfig, AttributeValues values) throws Exception {
        return adapter.delete(sourceConfig, values);
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws Exception {
        return adapter.getLastChangeNumber(sourceConfig);
    }

    public PenroseSearchResults getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws Exception {
        return adapter.getChanges(sourceConfig, lastChangeNumber);
    }

    public Object openConnection() throws Exception {
        return adapter.openConnection();
    }
}
