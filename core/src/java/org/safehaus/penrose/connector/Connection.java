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
package org.safehaus.penrose.connector;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class Connection implements ConnectionMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    protected PenroseConfig penroseConfig;
    protected PenroseContext penroseContext;

    protected ConnectionConfig connectionConfig;
    protected AdapterConfig adapterConfig;
    protected Adapter adapter;

    public Connection(ConnectionConfig connectionConfig, AdapterConfig adapterConfig) {
        this.connectionConfig = connectionConfig;
        this.adapterConfig = adapterConfig;
    }

    public String getName() {
        return connectionConfig.getName();
    }

    public void init() throws Exception {

        String adapterClass = adapterConfig.getAdapterClass();
        Class clazz = Class.forName(adapterClass);
        adapter = (Adapter)clazz.newInstance();

        adapter.setPenroseConfig(penroseConfig);
        adapter.setPenroseContext(penroseContext);
        adapter.setAdapterConfig(adapterConfig);
        adapter.setConnection(this);

        adapter.init();
    }

    public void close() throws Exception {
        if (adapter != null) adapter.dispose();
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

    public Map getParameters() {
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
            SourceConfig sourceConfig,
            RDN pk,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {
        adapter.add(sourceConfig, pk, sourceValues);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            SourceConfig sourceConfig,
            RDN pk,
            String password,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        adapter.bind(sourceConfig, pk, password);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            SourceConfig sourceConfig,
            RDN pk,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        adapter.delete(sourceConfig, pk);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            SourceConfig sourceConfig,
            RDN pk,
            Collection modifications,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        adapter.modify(sourceConfig, pk, modifications);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            SourceConfig sourceConfig,
            RDN oldPk,
            RDN newPk,
            boolean deleteOldRdn,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        adapter.modrdn(sourceConfig, oldPk, newPk, deleteOldRdn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceConfig sourceConfig,
            Collection primaryKeys,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        adapter.search(partition, entryMapping, sourceMapping, sourceConfig, request, response);
    }

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            Collection primaryKeys,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        adapter.search(partition, entryMapping, sourceMappings, request, response);
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws Exception {
        return adapter.getLastChangeNumber(sourceConfig);
    }

    public SearchResponse getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws Exception {
        return adapter.getChanges(sourceConfig, lastChangeNumber);
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
}
