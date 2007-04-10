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

import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager implements ConnectionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public Map<String,Map<String,Connection>> connections = new TreeMap<String,Map<String,Connection>>();

    public void init(Partition partition, ConnectionConfig connectionConfig) throws Exception {

        Connection connection = getConnection(partition, connectionConfig.getName());
        if (connection != null) return;

        log.debug("Initializing connection "+connectionConfig.getName()+".");

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name");

        AdapterConfig adapterConfig = penroseConfig.getAdapterConfig(adapterName);
        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

        connection = new Connection(partition, connectionConfig, adapterConfig);
        connection.setPenroseConfig(penroseConfig);
        connection.setPenroseContext(penroseContext);

        addConnection(partition.getName(), connection);
    }

    public void start() throws Exception {
        for (Iterator i=connections.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)connections.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Connection connection = (Connection)map.get(name);

                log.debug("Starting "+name+" connection.");
                try {
                    connection.init();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public void stop() throws Exception {
        for (Iterator i=connections.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map<String,Connection> map = (Map<String,Connection>)connections.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Connection connection = (Connection)map.get(name);

                log.debug("Closing "+name+" connection.");
                try {
                    connection.close();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public void addConnection(String partitionName, Connection connection) {
        Map<String,Connection> map = (Map<String,Connection>)connections.get(partitionName);
        if (map == null) {
            map = new TreeMap<String,Connection>();
            connections.put(partitionName, map);
        }
        map.put(connection.getName(), connection);
    }

    public Connection getConnection(Partition partition, String connectionName) throws Exception {
        Map<String,Connection> map = (Map<String,Connection>)connections.get(partition.getName());
        if (map == null) return null;
        return (Connection)map.get(connectionName);
    }

    public Collection<String> getPartitionNames() {
        return new ArrayList(connections.keySet()); // return Serializable list
    }

    public Collection getConnectionNames(String partitionName) {
        Map<String,Connection> map = (Map<String,Connection>)connections.get(partitionName);
        if (map == null) return new ArrayList();
        return new ArrayList<String>(map.keySet()); // return Serializable list
    }

    public Connection removeConnection(String partitionName, String connectionName) {
        Map<String,Connection> map = (Map<String,Connection>)connections.get(partitionName);
        if (map == null) return null;
        return (Connection)map.remove(connectionName);
    }

    public void clear() {
        connections.clear();
    }

    public Object openConnection(Partition partition, String connectionName) throws Exception {
        Connection connection = getConnection(partition, connectionName);
        return connection.openConnection();
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