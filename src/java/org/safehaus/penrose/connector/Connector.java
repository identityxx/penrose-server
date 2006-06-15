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

import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.util.PasswordUtil;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.BasicAttribute;
import javax.naming.directory.Attribute;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.DirContext;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Connector {

    static Logger log = Logger.getLogger(Connector.class);

    public final static String DEFAULT_CACHE_CLASS = SourceCache.class.getName();

    private PenroseConfig penroseConfig;
    private ConnectorConfig connectorConfig;
    private ConnectionManager connectionManager;
    private PartitionManager partitionManager;
    private SourceCache sourceCache;

    private ThreadManager threadManager;
    private boolean stopping = false;

    private Map locks = new HashMap();
    private Queue queue = new Queue();

    public Connector() {
    }

    public void init() throws Exception {
        CacheConfig cacheConfig = penroseConfig.getSourceCacheConfig();
        String cacheClass = cacheConfig.getCacheClass() == null ? DEFAULT_CACHE_CLASS : cacheConfig.getCacheClass();

        log.debug("Initializing source cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        sourceCache = (SourceCache)clazz.newInstance();

        sourceCache.setCacheConfig(cacheConfig);
        sourceCache.setConnector(this);
        sourceCache.setPenroseConfig(penroseConfig);
        sourceCache.setConnectionManager(connectionManager);
        sourceCache.setPartitionManager(partitionManager);
        sourceCache.setThreadManager(threadManager);
    }

    public void start() throws Exception {

        String s = connectorConfig.getParameter(ConnectorConfig.THREAD_POOL_SIZE);
        int threadPoolSize = s == null ? ConnectorConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            addPartition(partition);
        }
    }

    public boolean isStopping() {
        return stopping;
    }

    public void stop() throws Exception {
        if (stopping) return;
        stopping = true;
    }


    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;
    }

    public void addPartition(Partition partition) throws Exception {

        Collection sourceConfigs = partition.getSourceConfigs();
        for (Iterator i=sourceConfigs.iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();
            sourceCache.create(sourceConfig);
        }
    }

    public Connection getConnection(Partition partition, String name) throws Exception {
        return (Connection)connectionManager.getConnection(partition, name);
    }

    public Row normalize(Row row) throws Exception {

        Row newRow = new Row();
        if (row == null) return newRow;

        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }

            newRow.set(name, value);
        }

        return newRow;
    }

    public synchronized MRSWLock getLock(SourceConfig sourceConfig) {
		String name = sourceConfig.getConnectionName() + "." + sourceConfig.getName();

		MRSWLock lock = (MRSWLock)locks.get(name);

		if (lock == null) lock = new MRSWLock(queue);
		locks.put(name, lock);

		return lock;
	}

    public int bind(Partition partition, SourceConfig sourceConfig, EntryMapping entry, Row pk, String password) throws Exception {

        log.debug("Binding as entry in "+sourceConfig.getName());

        MRSWLock lock = getLock(sourceConfig);
        lock.getReadLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Connection connection = getConnection(partition, sourceConfig.getConnectionName());
            int rc = connection.bind(sourceConfig, pk, password);

            return rc;

        } finally {
        	lock.releaseReadLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }
    }

    public int add(Partition partition, SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+sourceConfig.getName());
        log.debug("Values: "+sourceValues);

        MRSWLock lock = getLock(sourceConfig);
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Collection pks = TransformEngine.getPrimaryKeys(sourceConfig, sourceValues);

            // Add rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues)sourceValues.clone();
                newEntry.set(pk);
                log.debug("ADDING ROW: " + newEntry);

                // Add row to source table in the source database/directory
                Connection connection = getConnection(partition, sourceConfig.getConnectionName());
                int rc = connection.add(sourceConfig, pk, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                Filter filter = FilterTool.createFilter(pk);

                AttributeValues sv = connection.get(sourceConfig, pk);
                getSourceCache().put(sourceConfig, pk, sv);
            }
/*
            sourceValues.clear();
            Collection list = retrieve(sourceConfig, pks);
            log.debug("Added rows:");
            for (Iterator i=list.iterator(); i.hasNext(); ) {
                AttributeValues sv = (AttributeValues)i.next();
                sourceValues.add(sv);
                log.debug(" - "+sv);
            }
*/
            //getQueryCache(connectionConfig, sourceConfig).invalidate();

        } finally {
        	lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(Partition partition, SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {

        log.debug("Deleting entry in "+sourceConfig.getName());

        MRSWLock lock = getLock(sourceConfig);
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Collection pks = TransformEngine.getPrimaryKeys(sourceConfig, sourceValues);

            // Remove rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                Row key = normalize((Row)pk);
                AttributeValues oldEntry = (AttributeValues)sourceValues.clone();
                oldEntry.set(pk);
                log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = getConnection(partition, sourceConfig.getConnectionName());
                int rc = connection.delete(sourceConfig, pk);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                getSourceCache().remove(sourceConfig, pk);
            }

            //getQueryCache(connectionConfig, sourceConfig).invalidate();

        } finally {
            lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(
            Partition partition,
            SourceConfig sourceConfig,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues) throws Exception {

        log.debug("Modifying entry in " + sourceConfig.getName());

        MRSWLock lock = getLock(sourceConfig);
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Collection oldPKs = TransformEngine.getPrimaryKeys(sourceConfig, oldSourceValues);
            Collection newPKs = TransformEngine.getPrimaryKeys(sourceConfig, newSourceValues);

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Set removeRows = new HashSet(oldPKs);
            removeRows.removeAll(newPKs);
            log.debug("PKs to remove: " + removeRows);

            Set addRows = new HashSet(newPKs);
            addRows.removeAll(oldPKs);
            log.debug("PKs to add: " + addRows);

            Set replaceRows = new HashSet(oldPKs);
            replaceRows.retainAll(newPKs);
            log.debug("PKs to replace: " + replaceRows);

            Collection pks = new ArrayList();

            // Remove rows
            for (Iterator i = removeRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                Row key = normalize((Row)pk);
                AttributeValues oldEntry = (AttributeValues)oldSourceValues.clone();
                oldEntry.set(pk);
                //log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = getConnection(partition, sourceConfig.getConnectionName());
                int rc = connection.delete(sourceConfig, pk);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                getSourceCache().remove(sourceConfig, pk);
            }

            // Add rows
            for (Iterator i = addRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues)newSourceValues.clone();
                newEntry.set(pk);
                //log.debug("ADDING ROW: " + newEntry);

                // Add row to source table in the source database/directory
                Connection connection = getConnection(partition, sourceConfig.getConnectionName());
                int rc = connection.add(sourceConfig, pk, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                Filter filter = FilterTool.createFilter(pk);

                AttributeValues sv = connection.get(sourceConfig, pk);
                getSourceCache().put(sourceConfig, pk, sv);

                pks.add(pk);
            }

            // Replace rows
            for (Iterator i = replaceRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                Row key = normalize((Row)pk);
                AttributeValues oldEntry = (AttributeValues)oldSourceValues.clone();
                oldEntry.set(pk);
                AttributeValues newEntry = (AttributeValues)newSourceValues.clone();
                newEntry.set(pk);
                //log.debug("REPLACE ROW: " + oldEntry+" with "+newEntry);

                // Modify row from source table in the source database/directory
                Connection connection = getConnection(partition, sourceConfig.getConnectionName());
                Collection modifications = createModifications(oldEntry, newEntry);
                int rc = connection.modify(sourceConfig, pk, modifications);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                Filter filter = FilterTool.createFilter(pk);

                AttributeValues sv = connection.get(sourceConfig, pk);
                getSourceCache().remove(sourceConfig, pk);
                getSourceCache().put(sourceConfig, pk, sv);

                pks.add(pk);
            }

            newSourceValues.clear();

            PenroseSearchResults list = retrieve(partition, sourceConfig, pks);
            for (Iterator i=list.iterator(); i.hasNext(); ) {
                AttributeValues sv = (AttributeValues)i.next();
                newSourceValues.add(sv);
            }

            //getQueryCache(connectionConfig, sourceConfig).invalidate();

        } finally {
            lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public Collection createModifications(AttributeValues oldValues, AttributeValues newValues) throws Exception {

        Collection list = new ArrayList();

        Set addAttributes = new HashSet(newValues.getNames());
        addAttributes.removeAll(oldValues.getNames());

        log.debug("Values to add:");
        for (Iterator i=addAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection values = newValues.get(name);
            Attribute attribute = new BasicAttribute(name);

            for (Iterator j = values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                log.debug(" - "+name+": "+value);
                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute));
        }

        Set removeAttributes = new HashSet(oldValues.getNames());
        removeAttributes.removeAll(newValues.getNames());

        log.debug("Values to remove:");
        for (Iterator i=removeAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection values = newValues.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = values.iterator(); j.hasNext(); ) {
                Object value = j.next();
            	log.debug(" - "+name+": "+value);
                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attribute));
        }

        Set replaceAttributes = new HashSet(oldValues.getNames());
        replaceAttributes.retainAll(newValues.getNames());

        log.debug("Values to replace:");
        for (Iterator i=replaceAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Set set = (Set)newValues.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                Object value = j.next();
                log.debug(" - "+name+": "+value);
                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute));
        }

        return list;
    }

    /**
     * Search the data sources.
     */
    public PenroseSearchResults search(
            final Partition partition,
            final SourceConfig sourceConfig,
            final Filter filter)
            throws Exception {

        String method = sourceConfig.getParameter(SourceConfig.LOADING_METHOD);
        if (SourceConfig.SEARCH_AND_LOAD.equals(method)) { // search for PKs first then load full record

            log.debug("Searching source "+sourceConfig.getName()+" with filter "+filter);
            return searchAndLoad(partition, sourceConfig, filter);

        } else { // load full record immediately

            log.debug("Loading source "+sourceConfig.getName()+" with filter "+filter);
            return fullLoad(partition, sourceConfig, filter);
        }
    }

    /**
     * Check query cache, peroform search, store results in query cache.
     */
    public PenroseSearchResults searchAndLoad(
            Partition partition,
            SourceConfig sourceConfig,
            Filter filter)
            throws Exception {

        PenroseSearchResults results = new PenroseSearchResults();

        log.debug("Checking query cache for "+filter);
        Collection pks = getSourceCache().search(sourceConfig, filter);

        log.debug("Cached results: "+pks);

        if (pks == null) {
            log.debug("Searching source "+sourceConfig.getName()+" with filter "+filter);
            PenroseSearchResults sr = performSearch(partition, sourceConfig, filter);
            pks = sr.getAll();

            int rc = sr.getReturnCode();
            if (rc != 0) {
                log.debug("RC: "+rc);
                results.setReturnCode(rc);
            }

            log.debug("Storing query cache for: "+pks);
            getSourceCache().put(sourceConfig, filter, pks);
        }

        log.debug("Loading source "+sourceConfig.getName()+" with pks "+pks);
        load(partition, sourceConfig, pks, results);

        return results;

    }

    /**
     * Load then store in data cache.
     */
    public PenroseSearchResults fullLoad(Partition partition, SourceConfig sourceConfig, Filter filter) throws Exception {

        Collection pks = getSourceCache().search(sourceConfig, filter);

        if (pks != null) {
            PenroseSearchResults results = new PenroseSearchResults();
            load(partition, sourceConfig, pks, results);
            return results;

        } else {
            PenroseSearchResults results = new PenroseSearchResults();
            performLoad(partition, sourceConfig, filter, results);
            return results;
            //store(sourceConfig, values);
        }
    }

    /**
     * Check data cache then load.
     */
    public void load(
            final Partition partition,
            final SourceConfig sourceConfig,
            final Collection pks,
            final PenroseSearchResults results)
            throws Exception {

        if (pks == null || pks.isEmpty()) {
            results.close();
            return;
        }

        threadManager.execute(new Runnable() {
            public void run() {
                try {
                    Collection normalizedPks = new ArrayList();
                    for (Iterator i=pks.iterator(); i.hasNext(); ) {
                        Row pk = (Row)i.next();
                        Row npk = normalize(pk);
                        normalizedPks.add(npk);
                    }

                    log.debug("Checking data cache for "+normalizedPks);
                    Collection missingPks = new ArrayList();
                    Map loadedRows = getSourceCache().load(sourceConfig, normalizedPks, missingPks);

                    log.debug("Cached values: "+loadedRows.keySet());
                    results.addAll(loadedRows.values());

                    log.debug("Loading missing keys: "+missingPks);
                    PenroseSearchResults list = retrieve(partition, sourceConfig, missingPks);
                    results.addAll(list.getAll());

                    int rc = list.getReturnCode();
                    log.debug("RC: "+rc);
                    results.setReturnCode(rc);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                }

                results.close();
            }
        });
    }

    /**
     * Load then store in data cache.
     */
    public PenroseSearchResults retrieve(Partition partition, SourceConfig sourceConfig, Collection keys) throws Exception {

        if (keys.isEmpty()) {
            PenroseSearchResults sr = new PenroseSearchResults();
            sr.close();
            return sr;
        }

        Filter filter = FilterTool.createFilter(keys);

        PenroseSearchResults sr = new PenroseSearchResults();
        performLoad(partition, sourceConfig, filter, sr);

        //Collection values = new ArrayList();
        //values.addAll(sr.getAll());

        //store(sourceConfig, values);

        return sr;
    }

    public Row store(SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {
        Row pk = sourceConfig.getPrimaryKeyValues(sourceValues);
        Row npk = normalize(pk);

        //log.debug("Storing connector cache: "+pk);
        getSourceCache().put(sourceConfig, pk, sourceValues);

        Filter f = FilterTool.createFilter(npk);
        Collection c = new TreeSet();
        c.add(npk);

        //log.debug("Storing query cache "+f+": "+c);
        getSourceCache().put(sourceConfig, f, c);

        return npk;
    }

    public void store(SourceConfig sourceConfig, Collection values) throws Exception {

        Collection pks = new TreeSet();

        Collection uniqueFieldDefinitions = sourceConfig.getUniqueFieldConfigs();
        Collection uniqueKeys = new TreeSet();

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            Row npk = store(sourceConfig, sourceValues);
            pks.add(npk);

            for (Iterator j=uniqueFieldDefinitions.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                String fieldName = fieldConfig.getName();

                Object value = sourceValues.getOne(fieldName);

                Row uniqueKey = new Row();
                uniqueKey.set(fieldName, value);

                uniqueKeys.add(uniqueKey);
            }
        }

        if (!uniqueKeys.isEmpty()) {
            Filter f = FilterTool.createFilter(uniqueKeys);
            log.debug("Storing query cache "+f+": "+pks);
            getSourceCache().put(sourceConfig, f, pks);
        }

        if (pks.size() <= 10) {
            Filter filter = FilterTool.createFilter(pks);
            log.debug("Storing query cache "+filter+": "+pks);
            getSourceCache().put(sourceConfig, filter, pks);
        }
    }

    /**
     * Perform the search operation.
     */
    public PenroseSearchResults performSearch(Partition partition, SourceConfig sourceConfig, Filter filter) throws Exception {

        PenroseSearchResults results = new PenroseSearchResults();

        String s = sourceConfig.getParameter(SourceConfig.SIZE_LIMIT);
        int sizeLimit = s == null ? SourceConfig.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());
        PenroseSearchResults sr = new PenroseSearchResults();
        try {
            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setSizeLimit(sizeLimit);

            connection.search(sourceConfig, filter, sc, sr);

            log.debug("Search results:");
            for (Iterator i=sr.iterator(); i.hasNext();) {
                Row pk = (Row)i.next();
                Row npk = normalize(pk);
                log.debug(" - "+npk);
                results.add(npk);
            }

            results.setReturnCode(sr.getReturnCode());
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            
        } finally {
            results.close();
        }

        return results;
    }

    /**
     * Perform the load operation.
     */
    public void performLoad(
            final Partition partition,
            final SourceConfig sourceConfig,
            final Filter filter,
            final PenroseSearchResults results
            ) throws Exception {

        final PenroseSearchResults sr = new PenroseSearchResults();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    AttributeValues sourceValues = (AttributeValues)event.getObject();
                    store(sourceConfig, sourceValues);
                    results.add(sourceValues);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                }
            }
            public void pipelineClosed(PipelineEvent event) {
                results.setReturnCode(sr.getReturnCode());
                results.close();
            }
        });

        threadManager.execute(new Runnable() {
            public void run() {
                try {
                    String s = sourceConfig.getParameter(SourceConfig.SIZE_LIMIT);
                    int sizeLimit = s == null ? SourceConfig.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

                    Connection connection = getConnection(partition, sourceConfig.getConnectionName());

                    PenroseSearchControls sc = new PenroseSearchControls();
                    sc.setSizeLimit(sizeLimit);

                    connection.load(sourceConfig, filter, sc, sr);
/*
                    for (Iterator i=sr.iterator(); i.hasNext();) {
                        AttributeValues sourceValues = (AttributeValues)i.next();
                        store(sourceConfig, sourceValues);
                        results.add(sourceValues);
                    }

                    results.setReturnCode(sr.getReturnCode());
*/
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    //results.setReturnCode(LDAPException.OPERATIONS_ERROR);

                } finally {
                    //results.close();
                }
            }
        });
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public SourceCache getSourceCache() {
        return sourceCache;
    }

    public void setSourceCache(SourceCache sourceCache) {
        this.sourceCache = sourceCache;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }
}
