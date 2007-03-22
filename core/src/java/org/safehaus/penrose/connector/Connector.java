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
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.cache.SourceCacheManager;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Connector {

    static Logger log = LoggerFactory.getLogger(Connector.class);

    public final static String DEFAULT_CACHE_CLASS = SourceCacheManager.class.getName();

    private PenroseConfig penroseConfig;
    private ConnectorConfig connectorConfig;

    private ConnectionManager connectionManager;
    private PartitionManager partitionManager;
    private SourceCacheManager sourceCacheManager;

    private boolean stopping = false;

    public Connector() {
    }

    public void init() throws Exception {
        sourceCacheManager = new SourceCacheManager();
        sourceCacheManager.setCacheConfig(penroseConfig.getSourceCacheConfig());
        sourceCacheManager.setConnector(this);
    }

    public void start() throws Exception {
    }

    public boolean isStopping() {
        return stopping;
    }

    public void stop() throws Exception {
        if (stopping) return;
        stopping = true;
    }


    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;
    }

    public void addPartition(Partition partition) throws Exception {

        Collection sourceConfigs = partition.getSourceConfigs();
        for (Iterator i=sourceConfigs.iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();

            String connectorName = sourceConfig.getParameter("connectorName");
            connectorName = connectorName == null ? "DEFAULT" : connectorName;

            if (!connectorConfig.getName().equals(connectorName)) continue;

            sourceCacheManager.create(partition, sourceConfig);
        }
    }

    public Connection getConnection(Partition partition, String name) throws Exception {
        return (Connection)connectionManager.getConnection(partition, name);
    }

    public RDN normalize(RDN rdn) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        if (rdn == null) return rb.toRdn();

        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }

            rb.set(name, value);
        }

        return rb.toRdn();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        connection.add(partition, entryMapping, sourceMappings, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        connection.bind(partition, entryMapping, sourceMappings, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        connection.delete(partition, entryMapping, sourceMappings, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        connection.modify(partition, entryMapping, sourceMappings, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        connection.modrdn(partition, entryMapping, sourceMappings, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Partition partition,
            final EntryMapping entryMapping,
            final Collection sourceMappings,
            final AttributeValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        connection.search(partition, entryMapping, sourceMappings, sourceValues, request, response);
    }

    public RDN store(
            Partition partition,
            SourceConfig sourceConfig,
            AttributeValues sourceValues
    ) throws Exception {

        RDN pk = sourceConfig.getPrimaryKeyValues(sourceValues);
        //RDN pk = sourceValues.getRdn();
        RDN npk = normalize(pk);

        if (log.isDebugEnabled()) log.debug("Storing source cache: "+pk);
        getSourceCacheManager().put(partition, sourceConfig, pk, sourceValues);

        Filter f = FilterTool.createFilter(npk);
        Collection c = new TreeSet();
        c.add(npk);

        if (log.isDebugEnabled()) log.debug("Storing filter cache "+f+": "+c);
        getSourceCacheManager().put(partition, sourceConfig, f, c);

        return npk;
    }

    public void store(
            Partition partition,
            SourceConfig sourceConfig,
            Collection values
    ) throws Exception {

        Collection pks = new TreeSet();

        Collection uniqueFieldDefinitions = sourceConfig.getUniqueFieldConfigs();
        Collection uniqueKeys = new TreeSet();

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            RDN npk = store(partition, sourceConfig, sourceValues);
            pks.add(npk);

            RDNBuilder rb = new RDNBuilder();
            for (Iterator j=uniqueFieldDefinitions.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                String fieldName = fieldConfig.getName();

                Object value = sourceValues.getOne(fieldName);

                rb.clear();
                rb.set(fieldName, value);

                uniqueKeys.add(rb.toRdn());
            }
        }

        if (!uniqueKeys.isEmpty()) {
            Filter f = FilterTool.createFilter(uniqueKeys);
            if (log.isDebugEnabled()) log.debug("Storing query cache "+f+": "+pks);
            getSourceCacheManager().put(partition, sourceConfig, f, pks);
        }

        if (pks.size() <= 10) {
            Filter filter = FilterTool.createFilter(pks);
            if (log.isDebugEnabled()) log.debug("Storing query cache "+filter+": "+pks);
            getSourceCacheManager().put(partition, sourceConfig, filter, pks);
        }
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public SourceCacheManager getSourceCacheManager() {
        return sourceCacheManager;
    }

    public void setSourceCacheManager(SourceCacheManager sourceCacheManager) {
        this.sourceCacheManager = sourceCacheManager;
    }
}
