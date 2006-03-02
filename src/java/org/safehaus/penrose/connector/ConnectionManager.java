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
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.config.PenroseConfig;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager implements ConnectionManagerMBean {

    public Logger log = Logger.getLogger(ConnectionManager.class);

    private PenroseConfig penroseConfig;
    public Map connectionConfigs = new TreeMap();
    public Map connections = new TreeMap();

    private PartitionManager partitionManager;

    public ConnectionConfig getConnectionConfig(Partition partition, String connectionName) {
        return (ConnectionConfig)connectionConfigs.get(partition.getName()+"/"+connectionName);
    }

    public Collection getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public ConnectionConfig removeConnectionConfig(Partition partition, String connectionName) {
        return (ConnectionConfig)connectionConfigs.remove(partition.getName()+"/"+connectionName);
    }

    public void start() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            Collection connectionConfigs = partition.getConnectionConfigs();
            for (Iterator j = connectionConfigs.iterator(); j.hasNext();) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();

                init(partition, connectionConfig);
            }
        }
    }

    public void init(Partition partition, ConnectionConfig connectionConfig) throws Exception {

        String name = partition.getName()+"/"+connectionConfig.getName();
        Connection connection;

        if (connectionConfigs.get(name) == null) {

            connectionConfigs.put(name, connectionConfig);

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null) throw new Exception("Missing adapter name");

            AdapterConfig adapterConfig = penroseConfig.getAdapterConfig(adapterName);
            if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

            connection = new Connection(connectionConfig, adapterConfig);

            connections.put(name, connection);
            
        } else {
            connection = (Connection)connections.get(name);
        }

        log.debug("Initializing "+name+" connection.");
        connection.init();
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
        return connectionConfigs.keySet();
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

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }
}