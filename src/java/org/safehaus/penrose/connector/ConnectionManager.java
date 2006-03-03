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
import org.safehaus.penrose.partition.Partition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager implements ConnectionManagerMBean {

    public Logger log = Logger.getLogger(ConnectionManager.class);

    public Map connections = new TreeMap();

    public void init(Partition partition, ConnectionConfig connectionConfig, AdapterConfig adapterConfig) throws Exception {

        String name = partition.getName()+"/"+connectionConfig.getName();
        Connection connection = (Connection)connections.get(name);
        if (connection != null) return;

        log.debug("Initializing "+name+" connection.");
        connection = new Connection(connectionConfig, adapterConfig);
        addConnection(partition, connection);
    }

    public void addConnection(Partition partition, Connection connection) {
        String name = partition.getName()+"/"+connection.getName();
        connections.put(name, connection);
    }

    public void clear() {
        connections.clear();
    }

    public void start() throws Exception {
        for (Iterator i=connections.values().iterator(); i.hasNext(); ) {
            Connection connection = (Connection)i.next();

            log.debug("Starting "+connection.getConnectionName()+" connection.");
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

    public Connection getConnection(Partition partition, String connectionName) throws Exception {
        String partitionName = partition == null ? "DEFAULT" : partition.getName();
        return (Connection)connections.get(partitionName+"/"+connectionName);
    }

    public Collection getConnectionNames() {
        return connections.keySet();
    }
    
    public Collection getConnections() {
        return connections.values();
    }

    public Object openConnection(String connectionName) throws Exception {
        return openConnection(null, connectionName);
    }

    public Object openConnection(Partition partition, String connectionName) throws Exception {
        Connection connection = getConnection(partition, connectionName);
        return connection.openConnection();
    }
}