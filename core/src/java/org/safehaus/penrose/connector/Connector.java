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

import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Connector {

    public static Logger log = LoggerFactory.getLogger(Connector.class);

    private PenroseConfig penroseConfig;
    private ConnectorConfig connectorConfig;

    private ConnectionManager connectionManager;
    private PartitionManager partitionManager;

    private boolean stopping = false;

    public Connector() {
    }

    public void init() throws Exception {
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

        Collection<SourceConfig> sourceConfigs = partition.getSources().getSourceConfigs();
        for (SourceConfig sourceConfig : sourceConfigs) {

            String connectorName = sourceConfig.getParameter("connectorName");
            connectorName = connectorName == null ? "DEFAULT" : connectorName;

            if (!connectorConfig.getName().equals(connectorName)) continue;
        }
    }

    public Connection getConnection(Partition partition, String name) throws Exception {
        return connectionManager.getConnection(partition, name);
    }

    public RDN normalize(RDN rdn) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        if (rdn == null) return rb.toRdn();

        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String) value).toLowerCase();
            }

            rb.set(name, value);
        }

        return rb.toRdn();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.add(
                session,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.bind(
                session,
                entryMapping, 
                sourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.compare(
                session,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.delete(
                session,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.modify(
                session,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.modrdn(
                session,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping entryMapping,
            final Collection<SourceRef> sourceRefs,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();
        Connection connection = source.getConnection();

        connection.search(
                session,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                response
        );
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
}
