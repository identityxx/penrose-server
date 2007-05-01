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
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceSync;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Iterator;

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

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public void dispose() throws Exception {
    }

    public boolean isJoinSupported() {
        return false;
    }

    public String getSyncClassName() {
        return SourceSync.class.getName();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Table
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create(Source source) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void rename(Source oldSource, Source newSource) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void drop(Source source) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void clean(Source source) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void status(Source source) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
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
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        add(source, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    public void bind(
            EntryMapping entryMapping,
            Collection sourceRefs,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        bind(source, request, response);
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
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        delete(source, request, response);
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
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        modify(source, request, response);
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
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        modrdn(source, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Source source,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
    }

    public void search(
            final EntryMapping entryMapping,
            final Collection sourceRefs,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        final SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Filter filter = request.getFilter();

        if (sourceValues != null) {
            for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection values = sourceValues.get(name);

                int p = name.indexOf(".");
                name = name.substring(p+1);
                
                for (Iterator j=values.iterator(); j.hasNext(); ) {
                    Object value = j.next();
                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    filter = FilterTool.appendAndFilter(filter, sf);
                }
            }
        }

        SearchRequest newRequest = new SearchRequest();
        newRequest.setFilter(filter);

        SearchResponse<SearchResult> newResponse = new SearchResponse<SearchResult>() {
            public void add(SearchResult result) throws Exception {

                SearchResult searchResult = new SearchResult();
                searchResult.setDn(result.getDn());
                searchResult.setEntryMapping(entryMapping);
                searchResult.setSourceAttributes(sourceRef.getAlias(), result.getAttributes());

                response.add(searchResult);
            }
            public void close() throws Exception {
                response.close();
            }
        };

        search(source, newRequest, newResponse);
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
