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
import org.safehaus.penrose.adapter.AdapterConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager implements ConnectionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Map connections = new TreeMap();

    public String getStatus(String partitionName, String connectionName) throws Exception {

        Map map = (Map)connections.get(partitionName);
        if (map == null) {
            log.debug("Partition "+partitionName+" not found");
            return null;
        }

        Connection connection = (Connection)map.get(connectionName);
        if (connection == null) {
            log.debug("Connection "+connectionName+" not found");
            return null;
        }

        return connection.getStatus();
    }

    public void clear() {
        connections.clear();
    }

    public Connection addConnection(Partition partition, ConnectionConfig connectionConfig, AdapterConfig adapterConfig) throws Exception {

        String name = partition.getName()+"/"+connectionConfig.getName();
        log.info("Adding connection "+name+".");

        Connection connection = new Connection(connectionConfig, adapterConfig);

        Map map = (Map)connections.get(partition.getName());
        if (map == null) {
            map = new TreeMap();
            connections.put(partition.getName(), map);
        }
        map.put(connectionConfig.getName(), connection);

        return connection;
    }

    public void start(String partitionName, String connectionName) throws Exception {
        Map map = (Map)connections.get(partitionName);
        if (map == null) {
            log.debug("Partition "+partitionName+" not found");
            return;
        }

        Connection connection = (Connection)map.get(connectionName);
        if (connection == null) {
            log.debug("Connection "+connectionName+" not found");
            return;
        }

        log.info("Starting connection "+connectionName+".");
        connection.start();
    }

    public void stop(String partitionName, String connectionName) throws Exception {
        Map map = (Map)connections.get(partitionName);
        if (map == null) {
            log.debug("Partition "+partitionName+" not found");
            return;
        }

        Connection connection = (Connection)map.get(connectionName);
        if (connection == null) {
            log.debug("Connection "+connectionName+" not found");
            return;
        }

        log.info("Stopping connection "+connectionName+".");
        connection.stop();
    }

    public void restart(String partitionName, String connectionName) throws Exception {
        Map map = (Map)connections.get(partitionName);
        if (map == null) {
            log.debug("Partition "+partitionName+" not found");
            return;
        }

        Connection connection = (Connection)map.get(connectionName);
        if (connection == null) {
            log.debug("Connection "+connectionName+" not found");
            return;
        }

        log.info("Stopping connection "+connectionName+".");
        connection.stop();

        log.info("Starting connection "+connectionName+".");
        connection.start();
    }

    public Connection getConnection(Partition partition, String connectionName) throws Exception {
        String partitionName = partition == null ? "DEFAULT" : partition.getName();
        return getConnection(partitionName, connectionName);
    }

    public Connection getConnection(String partitionName, String connectionName) throws Exception {
        Map map = (Map)connections.get(partitionName);
        if (map == null) return null;
        return (Connection)map.get(connectionName);
    }

    public ConnectionConfig getConnectionConfig(String partitionName, String connectionName) throws Exception {
        Connection connection = getConnection(partitionName, connectionName);
        if (connection == null) return null;
        return connection.getConnectionConfig();
    }

    public Collection getPartitionNames() {
        return new ArrayList(connections.keySet()); // return Serializable list
    }

    public Collection getConnectionNames(String partitionName) {
        Map map = (Map)connections.get(partitionName);
        if (map == null) return new ArrayList();
        return new ArrayList(map.keySet()); // return Serializable list
    }

    public Object openConnection(Partition partition, String connectionName) throws Exception {
        Connection connection = getConnection(partition, connectionName);
        return connection.openConnection();
    }
}