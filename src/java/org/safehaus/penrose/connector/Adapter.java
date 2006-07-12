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
package org.safehaus.penrose.connector;


import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.partition.SourceConfig;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata 
 */
public abstract class Adapter {

    public Logger log = LoggerFactory.getLogger(getClass());

    private AdapterConfig adapterConfig;
    private Connection connection;

    /**
     * Initialize.
     *
     * @throws Exception
     */
    public void init() throws Exception {
    }

    public void dispose() throws Exception {
    }

	/**
	 * Bind.
	 * 
	 * @throws Exception
	 */
    public int bind(SourceConfig sourceConfig, Row pk, String password) throws Exception {
        return LDAPException.INVALID_CREDENTIALS;
    }
    
    /**
     * Search.
     *
     * @param results Rows
     * @throws Exception
     */
    public void search(SourceConfig sourceConfig, Filter filter, PenroseSearchControls sc, PenroseSearchResults results) throws Exception {
        results.close();
    }

    /**
     * Load.
     *
     * @param results AttributeValues
     * @throws Exception
     */
    public void load(SourceConfig sourceConfig, Filter filter, PenroseSearchControls sc, PenroseSearchResults results) throws Exception {
        results.close();
    }

    /**
     * Add.
     * 
     * @throws Exception
     */
    public int add(SourceConfig sourceConfig, Row pk, AttributeValues sourceValues) throws Exception {
        return LDAPException.OPERATIONS_ERROR;
    }
    
    /**
     * Modify.
     * 
     * @throws Exception
     */
    public int modify(SourceConfig sourceConfig, Row pk, Collection modifications) throws Exception {
        return LDAPException.OPERATIONS_ERROR;
    }

    /**
     * Delete.
     * 
     * @throws Exception
     */
    public int delete(SourceConfig sourceConfig, Row pk) throws Exception {
        return LDAPException.OPERATIONS_ERROR;
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws Exception {
        return 0;
    }

    public PenroseSearchResults getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws Exception {
        return null;
    }

    public Object openConnection() throws Exception {
        return null;
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getParameter(String name) {
        return connection.getParameter(name);
    }

    public Map getParameters() {
        return connection.getParameters();
    }

    public Collection getParameterNames() {
        return connection.getParameterNames();
    }

    public String removeParameter(String name) {
        return connection.removeParameter(name);
    }

    public String getAdapterName() {
        return adapterConfig.getName();
    }

    public String getConnectionName() {
        return connection.getConnectionName();
    }

    public Filter convert(EntryMapping entryMapping, Filter filter) throws Exception {

        if (filter instanceof SubstringFilter) {
            return convert(entryMapping, (SubstringFilter)filter);
        } else {
            return filter;
        }
    }

    public Filter convert(EntryMapping entryMapping, SubstringFilter filter) throws Exception {
        return filter;
    }
}
