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


import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.SearchResults;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata 
 */
public abstract class Adapter {

    Logger log = Logger.getLogger(getClass());

    private AdapterConfig adapterConfig;
    private Connection connection;

    public void init(AdapterConfig adapterConfig, Connection connection) throws Exception {
        this.adapterConfig = adapterConfig;
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
    public abstract int bind(SourceDefinition sourceDefinition, AttributeValues values, String password) throws Exception;
    
    /**
     * Search.
     *
     * @throws Exception
     */
    public abstract SearchResults search(SourceDefinition sourceDefinition, Filter filter, long sizeLimit) throws Exception;

    /**
     * Load.
     *
     * @throws Exception
     */
    public abstract SearchResults load(SourceDefinition sourceDefinition, Filter filter, long sizeLimit) throws Exception;

    /**
     * Add.
     * 
     * @throws Exception
     */
    public abstract int add(SourceDefinition sourceDefinition, AttributeValues values) throws Exception;
    
    /**
     * Modify.
     * 
     * @throws Exception
     */
    public abstract int modify(SourceDefinition sourceDefinition, AttributeValues oldValues, AttributeValues newValues) throws Exception;

    /**
     * Modify RDN.
     *
     * @throws Exception
     */
    public abstract int modrdn(SourceDefinition sourceDefinition, Row oldValues, Row newValues) throws Exception;

    /**
     * Delete.
     * 
     * @throws Exception
     */
    public abstract int delete(SourceDefinition sourceDefinition, AttributeValues values) throws Exception;

    public abstract int getLastChangeNumber(SourceDefinition sourceDefinition) throws Exception;

    public abstract SearchResults getChanges(SourceDefinition sourceDefinition, int lastChangeNumber) throws Exception;

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

    public static Row getPrimaryKeyValues(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Row pk = new Row();

        Collection fields = sourceDefinition.getPrimaryKeyFieldDefinitions();

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            String name = fieldDefinition.getName();

            Collection values = sourceValues.get(name);
            if (values == null) return null;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) return null;

            Object value = iterator.next();

            pk.set(name, value);
        }

        return pk;
    }

}
