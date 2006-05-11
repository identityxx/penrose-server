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

import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.thread.ThreadManager;

import java.util.TreeMap;
import java.util.Map;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ConnectorManager {

    Map connectors = new TreeMap();

    private PenroseConfig penroseConfig;
    private ConnectionManager connectionManager;
    private PartitionManager partitionManager;
    private ThreadManager threadManager;

    public ConnectorManager() {
    }

    public void init(ConnectorConfig connectorConfig) throws Exception {

        Class clazz = Class.forName(connectorConfig.getConnectorClass());
        Connector connector = (Connector)clazz.newInstance();

        connector.setConnectorConfig(connectorConfig);
        connector.setPenroseConfig(penroseConfig);
        connector.setConnectionManager(connectionManager);
        connector.setPartitionManager(partitionManager);
        connector.setThreadManager(threadManager);
        
        connector.init();

        connectors.put(connectorConfig.getName(), connector);
    }

    public Connector getConnector(String name) {
        return (Connector)connectors.get(name);
    }

    public void clear() {
        connectors.clear();
    }

    public void start() throws Exception {
        for (Iterator i=connectors.values().iterator(); i.hasNext(); ) {
            Connector connector = (Connector)i.next();
            connector.start();
        }
    }

    public void stop() throws Exception {
        for (Iterator i=connectors.values().iterator(); i.hasNext(); ) {
            Connector connector = (Connector)i.next();
            connector.stop();
        }
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }
}
