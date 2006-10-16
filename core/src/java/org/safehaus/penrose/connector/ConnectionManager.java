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

import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager implements ConnectionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Map connectionConfigs = new TreeMap();
    public Map adapterConfigs = new TreeMap();

    public Map connections = new TreeMap();

    public void init(Partition partition, ConnectionConfig connectionConfig, AdapterConfig adapterConfig) throws Exception {

        String name = partition.getName()+"/"+connectionConfig.getName();
        if (connectionConfigs.containsKey(name)) return;

        log.debug("Registering "+name+" connection.");
        connectionConfigs.put(name, connectionConfig);
        adapterConfigs.put(name, adapterConfig);
    }

    public void clear() {
        connections.clear();
    }

    public void start() throws Exception {
        for (Iterator i=connectionConfigs.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            log.debug("Starting "+name+" connection.");
            try {
                ConnectionConfig connectionConfig = (ConnectionConfig )connectionConfigs.get(name);
                AdapterConfig adapterConfig = (AdapterConfig)adapterConfigs.get(name);

                Connection connection = new Connection(connectionConfig, adapterConfig);

                connection.init();
                connections.put(name, connection);

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void stop() throws Exception {
        for (Iterator i=connections.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Connection connection = (Connection)connections.get(name);

            log.debug("Closing "+connection.getConnectionName()+" connection.");
            try {
                connection.close();

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public Connection getConnection(Partition partition, String connectionName) throws Exception {
        String partitionName = partition == null ? "DEFAULT" : partition.getName();
        return (Connection)connections.get(partitionName+"/"+connectionName);
    }

    public Collection getConnectionNames() {
        return connections.keySet();
    }

    public Object openConnection(String connectionName) throws Exception {
        return openConnection(null, connectionName);
    }

    public Object openConnection(Partition partition, String connectionName) throws Exception {
        Connection connection = getConnection(partition, connectionName);
        return connection.openConnection();
    }
}