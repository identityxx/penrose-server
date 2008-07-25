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

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceContext;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class Connection {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected ConnectionConfig connectionConfig;
    protected ConnectionContext connectionContext;

    public Connection() {
    }

    public String getName() {
        return connectionConfig.getName();
    }

    public void init(
            ConnectionConfig connectionConfig,
            ConnectionContext connectionContext
    ) throws Exception {

        log.debug("Starting "+connectionConfig.getName()+" connection.");

        this.connectionConfig = connectionConfig;
        this.connectionContext = connectionContext;

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public boolean isJoinSupported() {
        return connectionContext.getAdapter().isJoinSupported();
    }

    public String getDescription() {
        return connectionConfig.getDescription();
    }

    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public Adapter getAdapter() {
        return connectionContext.getAdapter();
    }

    public String getAdapterName() {
        return connectionContext.getAdapter().getName();
    }

    public String getParameter(String name) {
        return connectionConfig.getParameter(name);
    }

    public Map<String,String> getParameters() {
        return connectionConfig.getParameters();
    }

    public Collection<String> getParameterNames() {
        return connectionConfig.getParameterNames();
    }

    public String removeParameter(String name) {
        return connectionConfig.removeParameter(name);
    }

    public String getConnectionName() {
        return connectionConfig.getName();
    }

    public Partition getPartition() {
        return connectionContext.getPartition();
    }

    public Source createSource(SourceConfig sourceConfig) throws Exception {

        Partition partition = connectionContext.getPartition();
        return createSource(partition, sourceConfig);
    }

    public Source createSource(Partition partition, SourceConfig sourceConfig) throws Exception {

        SourceContext sourceContext = new SourceContext();
        sourceContext.setPartition(partition);
        sourceContext.setConnection(this);

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        String className = sourceConfig.getSourceClass();

        if (className == null) {
            Adapter adapter = connectionContext.getAdapter();
            className = adapter.getSourceClassName();
        }

        Class clazz = cl.loadClass(className);

        Source source = (Source)clazz.newInstance();
        source.init(sourceConfig, sourceContext);

        return source;
    }

    public String getSourceClassName() throws Exception {
        return Source.class.getName();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Source source,
            AddRequest request,
            AddResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Source source,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Source source,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }
}
