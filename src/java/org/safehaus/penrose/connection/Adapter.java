/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.connection;


import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata 
 */
public abstract class Adapter {

    public Logger log = Logger.getLogger(Penrose.ADAPTER_LOGGER);

    private AdapterContext adapterContext;
    private AdapterConfig adapterConfig;
    private Connection connection;

    public void init(AdapterConfig adapterConfig, AdapterContext adapterContext, Connection connection) throws Exception {
        this.adapterConfig = adapterConfig;
        this.adapterContext = adapterContext;
        this.connection = connection;

        init();
    }

    /**
     * Initialize.
     *
     * @throws Exception
     */
    public abstract void init() throws Exception;

	/**
	 * Bind.
	 * 
	 * @throws Exception
	 */
    public abstract int bind(Source source, AttributeValues values, String password) throws Exception;
    
    /**
     * Search.
     *
     * @throws Exception
     */
    public abstract SearchResults search(Source source, Filter filter) throws Exception;

    /**
     * Add.
     * 
     * @throws Exception
     */
    public abstract int add(Source source, AttributeValues values) throws Exception;
    
    /**
     * Modify.
     * 
     * @throws Exception
     */
    public abstract int modify(Source source, AttributeValues oldValues, AttributeValues newValues) throws Exception;

    /**
     * Delete.
     * 
     * @throws Exception
     */
    public abstract int delete(Source source, AttributeValues values) throws Exception;

    public AdapterContext getAdapterContext() {
        return adapterContext;
    }

    public void setAdapterContext(AdapterContext adapterContext) {
        this.adapterContext = adapterContext;
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getParameter(String name) {
        return connection.getParameter(name);
    }

    public Collection getParameterNames() {
        return connection.getParameterNames();
    }

    public String getAdapterName() {
        return adapterConfig.getAdapterName();
    }

    public String getConnectionName() {
        return connection.getConnectionName();
    }
}
