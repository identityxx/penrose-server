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
package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceManager implements SourceManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    Map sources = new TreeMap();

    private PenroseConfig penroseConfig;
    private ConnectionManager connectionManager;

    public SourceManager() {
    }

    public void init() throws Exception {
    }

    public Source create(Partition partition, SourceConfig sourceConfig) throws Exception {

        Source source = new Source();
        source.setSourceManager(this);
        source.setPartition(partition);
        source.setSourceConfig(sourceConfig);

        Connection connection = connectionManager.getConnection(partition, sourceConfig.getConnectionName());
        source.setConnection(connection);

        source.init();

        Map map = (Map)sources.get(partition.getName());
        if (map == null) {
            map = new TreeMap();
            sources.put(partition.getName(), map);
        }
        map.put(sourceConfig.getName(), source);

        return source;
    }

    public Source getSource(String partitionName, String sourceName) {
        Map map = (Map)sources.get(partitionName);
        if (map == null) return null;
        return (Source)map.get(sourceName);
    }

    public void clear() {
        sources.clear();
    }

    public void start() throws Exception {
        for (Iterator i=sources.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)sources.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                Source source = (Source)map.get(sourceName);
                source.start();
            }
        }
    }

    public void start(String partitionName, String sourceName) throws Exception {
        Source source = getSource(partitionName, sourceName);
        if (source == null) return;
        source.start();
    }

    public void stop() throws Exception {
        for (Iterator i=sources.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)sources.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                Source source = (Source)map.get(sourceName);
                source.stop();
            }
        }
    }

    public void stop(String partitionName, String sourceName) throws Exception {
        Source source = getSource(partitionName, sourceName);
        if (source == null) return;
        source.stop();
    }

    public void restart() throws Exception {
        stop();
        start();
    }

    public void restart(String partitionName, String sourceName) throws Exception {
        stop(partitionName, sourceName);
        start(partitionName, sourceName);
    }

    public String getStatus(String partitionName, String sourceName) throws Exception {
        Source source = getSource(partitionName, sourceName);
        if (source == null) return null;
        return source.getStatus();
    }

    public Collection getPartitionNames() {
        return new ArrayList(sources.keySet()); // return Serializable list
    }

    public Collection getSourceNames(String partitionName) {
        Map map = (Map)sources.get(partitionName);
        if (map == null) return new ArrayList();
        return new ArrayList(map.keySet()); // return Serializable list
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
}
