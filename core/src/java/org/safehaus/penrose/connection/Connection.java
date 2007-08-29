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
package org.safehaus.penrose.connection;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.adapter.AdapterContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class Connection implements ConnectionMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected ConnectionConfig connectionConfig;
    protected ConnectionContext connectionContext;

    protected AdapterConfig adapterConfig;
    protected Adapter adapter;

    protected Partition partition;
    protected PenroseConfig penroseConfig;
    protected PenroseContext penroseContext;

    public Connection() {
    }

    public String getName() {
        return connectionConfig.getName();
    }

    public void init(
            ConnectionConfig connectionConfig,
            ConnectionContext connectionContext,
            AdapterConfig adapterConfig
    ) throws Exception {

        this.connectionConfig = connectionConfig;
        this.connectionContext = connectionContext;

        this.adapterConfig = adapterConfig;

        partition = connectionContext.getPartition();
        penroseConfig = partition.getPartitionContext().getPenroseConfig();
        penroseContext = partition.getPartitionContext().getPenroseContext();

        log.debug("Starting "+connectionConfig.getName()+" connection.");

        String adapterClass = adapterConfig.getAdapterClass();
        ClassLoader cl = partition.getClassLoader();
        Class clazz = cl.loadClass(adapterClass);
        adapter = (Adapter)clazz.newInstance();

        AdapterContext adapterContext = new AdapterContext();
        adapterContext.setPartition(partition);
        adapterContext.setConnection(this);

        adapter.init(adapterConfig, adapterContext);

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
        log.debug("Stopping "+connectionConfig.getName()+" connection.");
        adapter.destroy();
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

    public Map<String,String> getParameters() {
        return connectionConfig.getParameters();
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Source source,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        adapter.add(session, source, request, response);
    }

    public void add(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        adapter.add(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Source source,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        adapter.bind(session, source, request, response);
    }

    public void bind(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        adapter.bind(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Source source,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        adapter.compare(session, source, request, response);
    }

    public void compare(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        adapter.compare(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Source source,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        adapter.delete(session, source, request, response);
    }

    public void delete(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        adapter.delete(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Source source,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        adapter.modify(session, source, request, response);
    }

    public void modify(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        adapter.modify(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Source source,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        adapter.modrdn(session, source, request, response);
    }

    public void modrdn(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        adapter.modrdn(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Source source,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        adapter.search(session, source, request, response);
    }

    public void search(
            Session session,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        adapter.search(session, entryMapping, sourceRefs, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Table
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create(Source source) throws Exception {
        adapter.create(source);
    }

    public void rename(Source oldSource, Source newSource) throws Exception {
        adapter.rename(oldSource, newSource);
    }

    public void drop(Source source) throws Exception {
        adapter.drop(source);
    }

    public void clean(Source source) throws Exception {
        adapter.clean(source);
    }

    public void status(Source source) throws Exception {
        adapter.status(source);
    }

    public long getCount(Source source) throws Exception {
        return adapter.getCount(source);
    }

    public Object openConnection() throws Exception {
        return adapter.openConnection();
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
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
