/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.adapter;


import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.connector.Connection;
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
    public void bind(SourceConfig sourceConfig, RDN pk, String password) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }
    
    /**
     * Search.
     *
     * @param partition
     * @param entryMapping
     * @param sourceMapping
     * @param results AttributeValues
     * @throws Exception
     */
    public void search(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceConfig sourceConfig,
            Filter filter,
            PenroseSearchControls searchControls,
            Results results
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    /**
     * Add.
     * 
     * @throws Exception
     */
    public void add(SourceConfig sourceConfig, RDN pk, AttributeValues sourceValues) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }
    
    /**
     * Modify.
     * 
     * @throws Exception
     */
    public void modify(SourceConfig sourceConfig, RDN pk, Collection modifications) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    /**
     * Modify.
     *
     * @throws Exception
     */
    public void modrdn(SourceConfig sourceConfig, RDN oldPk, RDN newPk, boolean deleteOldRdn) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    /**
     * Delete.
     * 
     * @throws Exception
     */
    public void delete(SourceConfig sourceConfig, RDN pk) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
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
