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

import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.source.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata 
 */
public abstract class Adapter {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected AdapterConfig adapterConfig;
    protected AdapterContext adapterContext;

    public void init(AdapterConfig adapterConfig, AdapterContext adapterContext) throws Exception {

        if (debug) log.debug("Creating "+adapterConfig.getName()+" adapter.");

        this.adapterConfig = adapterConfig;
        this.adapterContext = adapterContext;

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public String getName() {
        return adapterConfig.getName();
    }

    public boolean isJoinSupported() {
        return false;
    }

    public String getConnectionClassName() {
        return Connection.class.getName();
    }

    public String getSourceClassName() throws Exception {
        return Source.class.getName();
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
    }

    public AdapterContext getAdapterContext() {
        return adapterContext;
    }

    public void setAdapterContext(AdapterContext adapterContext) {
        this.adapterContext = adapterContext;
    }

    public Connection createConnection(
            ConnectionConfig connectionConfig,
            ConnectionContext connectionContext
    ) throws Exception {

        String className = getConnectionClassName();

        ClassLoader cl = connectionContext.getClassLoader();
        Class clazz = cl.loadClass(className);

        if (debug) log.debug("Creating "+className+".");
        Connection connection = (Connection)clazz.newInstance();

        connection.init(connectionConfig, connectionContext);

        return connection;
    }
}
