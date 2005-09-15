/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.connection;


import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata 
 */
public abstract class Adapter {

    Logger log = LoggerFactory.getLogger(getClass());

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
    public abstract SearchResults search(Source source, Filter filter, long sizeLimit) throws Exception;

    /**
     * Load.
     *
     * @throws Exception
     */
    public abstract SearchResults load(Source source, Filter filter, long sizeLimit) throws Exception;

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
