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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PersistentSourceCache extends SourceCache {

    ConnectionManager connectionManager;
    String connectionName;

    JDBCCache cache;

    public void init() throws Exception {
        super.init();

        connectionManager = sourceCacheManager.getConnectionManager();
        connectionName = getParameter("connection");

        String tableName = getTableName();

        cache = new JDBCCache(tableName, sourceConfig);
        cache.setPartition(partition);
        cache.setConnectionManager(connectionManager);
        cache.setConnectionName(connectionName);
        cache.setSize(size);
        cache.setExpiration(expiration);
        cache.init();
    }

    public String getTableName() {
        String tableName = partition.getPartitionConfig().getName()+"_"+sourceConfig.getName();
        tableName = tableName.replace(' ', '_');
        return tableName;
    }

    public void create() throws Exception {
        cache.create();
    }

    public void clean() throws Exception {
        cache.clean();
    }

    public void drop() throws Exception {
        cache.drop();
    }

    public Object get(Object key) throws Exception {
        Row pk = (Row)key;

        return cache.get(pk);
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }
    
    public Map load(Collection keys, Collection missingKeys) throws Exception {
        return cache.load(keys, missingKeys);
    }

    public void load() throws Exception {
/*
        String s = sourceConfig.getParameter(SourceConfig.AUTO_REFRESH);
        boolean autoRefresh = s == null ? SourceConfig.DEFAULT_AUTO_REFRESH : new Boolean(s).booleanValue();

        if (!autoRefresh) return;
*/
        String s = sourceConfig.getParameter(SourceConfig.SIZE_LIMIT);
        final int sizeLimit = s == null ? SourceConfig.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        log.info("Loading cache for "+sourceConfig.getName()+"...");

        ConnectionManager connectionManager = sourceCacheManager.getConnectionManager();
        final Connection connection = connectionManager.getConnection(partition, sourceConfig.getConnectionName());

        final PenroseSearchResults sr = new PenroseSearchResults();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    AttributeValues sourceValues = (AttributeValues)event.getObject();
                    Row pk = sourceConfig.getPrimaryKeyValues(sourceValues);
                    //Row pk = sourceValues.getRdn();
                    log.info("Storing "+pk+" in source cache");
                    put(pk, sourceValues);
                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                try {
                    int lastChangeNumber = connection.getLastChangeNumber(sourceConfig);
                    log.info("Last change number for "+sourceConfig.getName()+": "+lastChangeNumber);
                    setLastChangeNumber(lastChangeNumber);
                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            }
        });

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setSizeLimit(sizeLimit);

        connection.load(sourceConfig, null, null, sc, sr);

        sr.close();
    }

    public Collection search(Filter filter) throws Exception {
        return cache.search(filter);
    }

    public void put(Object key, Object object) throws Exception {
        Row pk = (Row)key;
        AttributeValues sourceValues = (AttributeValues)object;

        cache.put(pk, sourceValues);
    }

    public void remove(Object key) throws Exception {
        Row pk = (Row)key;

        cache.remove(pk);
    }

    public int getLastChangeNumber() throws Exception {
        return cache.getLastChangeNumber();
    }

    public void setLastChangeNumber(int lastChangeNumber) throws Exception {
        cache.setLastChangeNumber(lastChangeNumber);
    }

    public void put(Filter filter, Collection pks) throws Exception {
    }

    public void invalidate() throws Exception {
    }
}
