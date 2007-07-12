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

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.adapter.Adapter;
import org.apache.log4j.*;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceSyncManager {

    public Logger log = Logger.getLogger(getClass());

    public final static Collection<SourceSync> EMPTY_SOURCE_SYNCS = new ArrayList<SourceSync>();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public Map<String,Map<String, SourceSync>> sourceSyncs = new LinkedHashMap<String,Map<String, SourceSync>>();

    public SourceSyncManager() throws Exception {
    }

    public void init(Partition partition, SourceSyncConfig sourceSyncConfig) throws Exception {
        SourceSync sourceSync = getSourceSync(partition, sourceSyncConfig.getName());
        if (sourceSync != null) return;

        log.debug("Initializing source sync "+sourceSyncConfig.getName()+".");

        SourceConfig sourceConfig = sourceSyncConfig.getSourceConfig();

        ConnectionManager connectionManager = penroseContext.getConnectionManager();
        Connection connection = connectionManager.getConnection(partition, sourceConfig.getConnectionName());
        Adapter adapter = connection.getAdapter();
        String syncClassName = adapter.getSyncClassName();
        Class clazz = Class.forName(syncClassName);

        sourceSync = (SourceSync)clazz.newInstance();

        sourceSync.setSourceSyncConfig(sourceSyncConfig);
        sourceSync.setPartition(partition);
        sourceSync.setPenroseConfig(penroseConfig);
        sourceSync.setPenroseContext(penroseContext);
        sourceSync.init();

        addSourceSync(partition, sourceSync);
    }

    public void start() throws Exception {
        for (Iterator i= sourceSyncs.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map) sourceSyncs.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                SourceSync sourceSync = (SourceSync)map.get(name);

                log.debug("Starting "+name+" source sync.");
                try {
                    sourceSync.start();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public void stop() throws Exception {
        for (Iterator i= sourceSyncs.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map<String,SourceSync> map = (Map<String,SourceSync>) sourceSyncs.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                SourceSync sourceSync = (SourceSync)map.get(name);

                log.debug("Closing "+name+" source sync.");
                try {
                    sourceSync.stop();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public Collection<SourceSync> getSourceSyncs(Partition partition) {
        Map<String, SourceSync> map = (Map<String, SourceSync>) sourceSyncs.get(partition.getName());
        if (map == null) return EMPTY_SOURCE_SYNCS;
        return map.values();
    }

    public SourceSync getSourceSync(Partition partition, String sourceName) throws Exception {
        Map<String, SourceSync> map = (Map<String, SourceSync>) sourceSyncs.get(partition.getName());
        if (map == null) return null;
        return (SourceSync)map.get(sourceName);
    }

    public void addSourceSync(Partition partition, SourceSync sourceSync) {
        Map<String, SourceSync> map = (Map<String, SourceSync>) sourceSyncs.get(partition.getName());
        if (map == null) {
            map = new LinkedHashMap<String, SourceSync>();
            sourceSyncs.put(partition.getName(), map);
        }
        map.put(sourceSync.getName(), sourceSync);
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
