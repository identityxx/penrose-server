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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.thread.ThreadManager;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceCacheStorage {

    Logger log = Logger.getLogger(getClass());

    ThreadManager threadManager;

    Partition partition;
    Connector connector;
    SourceConfig sourceConfig;

    CacheConfig cacheConfig;

    int size;
    int expiration; // minutes

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public Collection getParameterNames() {
        return cacheConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return cacheConfig.getParameter(name);
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public SourceConfig getSourceDefinition() {
        return sourceConfig;
    }

    public void setSourceDefinition(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public void init(CacheConfig cacheConfig) throws Exception {
        this.cacheConfig = cacheConfig;

        String s = cacheConfig.getParameter(CacheConfig.CACHE_SIZE);
        size = s == null ? CacheConfig.DEFAULT_CACHE_SIZE : Integer.parseInt(s);

        s = cacheConfig.getParameter(CacheConfig.CACHE_EXPIRATION);
        expiration = s == null ? CacheConfig.DEFAULT_CACHE_EXPIRATION : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {

        String s = sourceConfig.getParameter(SourceConfig.DATA_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = sourceConfig.getParameter(SourceConfig.DATA_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);
    }

    public int getLastChangeNumber() throws Exception {
        return -1;
    }

    public void setLastChangeNumber(int lastChangeNumber) throws Exception {
    }

    public Map load(Collection filters, Collection missingKeys) throws Exception {
        missingKeys.addAll(filters);
        return new HashMap();
    }

    public void create() throws Exception {
    }

    public void clean() throws Exception {
    }

    public void drop() throws Exception {
    }

    public Collection search(Filter filter) throws Exception {
        return null;
    }

    public Object get(Object key) throws Exception {
        return null;
    }

    public void put(Object key, Object object) throws Exception {
    }

    public void put(Filter filter, Collection pks) throws Exception {
    }

    public void invalidate() throws Exception {
    }

    public void remove(Object key) throws Exception {
    }

    public Map getExpired() throws Exception {
        return null;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
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

        final Connection connection = connector.getConnection(partition, sourceConfig.getConnectionName());

        final PenroseSearchResults sr = new PenroseSearchResults();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    AttributeValues sourceValues = (AttributeValues)event.getObject();
                    Row pk = sourceConfig.getPrimaryKeyValues(sourceValues);
                    log.info("Source Cache <= "+pk);
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

        connection.load(sourceConfig, null, sizeLimit, sr);
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }
}
