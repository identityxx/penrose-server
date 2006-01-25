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

import org.apache.log4j.Logger;
import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.config.PenroseConfig;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager {

    public Logger log = Logger.getLogger(ConnectionManager.class);

    private PenroseConfig penroseConfig;
    public Map connectionConfigs = new TreeMap();
    public Map connections = new TreeMap();

    public void addConnectionConfig(ConnectionConfig connectionConfig) throws Exception {

        String name = connectionConfig.getName();
        if (connectionConfigs.get(name) != null) throw new Exception("Duplication connection "+name);

        connectionConfigs.put(name, connectionConfig);

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name");

        AdapterConfig adapterConfig = penroseConfig.getAdapterConfig(adapterName);
        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

        Connection connection = new Connection();
        connection.setConnectionConfig(connectionConfig);
        connection.setAdapterConfig(adapterConfig);

        connections.put(connectionConfig.getName(), connection);
    }

    public ConnectionConfig getConnectionConfig(String connectionName) {
        return (ConnectionConfig)connectionConfigs.get(connectionName);
    }

    public Collection getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public ConnectionConfig removeConnectionConfig(String connectionName) {
        return (ConnectionConfig)connectionConfigs.remove(connectionName);
    }

    public void start() throws Exception {
        for (Iterator i=connections.values().iterator(); i.hasNext(); ) {
            Connection connection = (Connection)i.next();
            log.debug("Initializing "+connection.getConnectionName()+" connection.");
            connection.init();
        }
    }

    public void stop() throws Exception {
        for (Iterator i=connections.values().iterator(); i.hasNext(); ) {
            Connection connection = (Connection)i.next();
            log.debug("Closing "+connection.getConnectionName()+" connection.");
            connection.close();
        }
    }

    public Connection getConnection(String connectionName) throws Exception {
        return (Connection)connections.get(connectionName);
    }

    public Object openConnection(String connectionName) throws Exception {
        Connection connection = getConnection(connectionName);
        return connection.openConnection();
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }
}