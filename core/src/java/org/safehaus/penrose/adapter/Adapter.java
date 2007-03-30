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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.ldap.*;
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

    protected PenroseConfig penroseConfig;
    protected PenroseContext penroseContext;

    protected Partition partition;
    protected AdapterConfig adapterConfig;
    protected Connection connection;

    /**
     * Initialize.
     *
     * @throws Exception
     */
    public void init() throws Exception {
    }

    public void dispose() throws Exception {
    }

    public boolean isJoinSupported() {
        return false;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Source source,
            AddRequest request,
            AddResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void add(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void delete(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void modify(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void modrdn(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Source source,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void search(
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public Long getLastChangeNumber(Source source) throws Exception {
        return null;
    }

    public SearchResponse getChanges(Source source, Long lastChangeNumber) throws Exception {
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

    public Filter convert(EntryMapping entryMapping, SubstringFilter filter) throws Exception {
        return filter;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
