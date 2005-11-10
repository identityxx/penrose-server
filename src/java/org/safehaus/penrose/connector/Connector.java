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

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.cache.ConnectorQueryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.ConnectorDataCache;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.config.ServerConfigReader;
import org.safehaus.penrose.config.ConfigReader;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.Adapter;
import org.safehaus.penrose.connection.AdapterConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Connector {

    static Logger log = Logger.getLogger(Connector.class);

    public final static int WAIT_TIMEOUT = 10000; // wait timeout is 10 seconds

    private ServerConfig serverConfig;
    private ConnectorConfig connectorConfig;

    private ThreadPool threadPool;
    private boolean stopping = false;

    private Map locks = new HashMap();
    private Queue queue = new Queue();

    private Map connections = new LinkedHashMap();
    public Collection configs = new ArrayList();

    private Map sourceFilterCaches = new TreeMap();
    private Map sourceDataCaches = new TreeMap();

    public void init(ServerConfig serverConfig, ConnectorConfig connectorConfig) throws Exception {
        this.serverConfig = serverConfig;
        this.connectorConfig = connectorConfig;
    }

    public void start() throws Exception {
        String s = connectorConfig.getParameter(ConnectorConfig.THREAD_POOL_SIZE);
        int threadPoolSize = s == null ? ConnectorConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        threadPool = new ThreadPool(threadPoolSize);
        execute(new RefreshThread(this));
    }

    public boolean isStopping() {
        return stopping;
    }

    public void execute(Runnable runnable) throws Exception {
        threadPool.execute(runnable);
    }

    public void stop() throws Exception {
        if (stopping) return;
        stopping = true;

        // wait for all the worker threads to finish
        threadPool.stopRequestAllWorkers();
    }

    public void addConfig(Config config) throws Exception {
        configs.add(config);

        for (Iterator i = config.getConnectionConfigs().iterator(); i.hasNext();) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null) throw new Exception("Missing adapter name");

            AdapterConfig adapterConfig = serverConfig.getAdapterConfig(adapterName);
            if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

            String adapterClass = adapterConfig.getAdapterClass();
            Class clazz = Class.forName(adapterClass);
            Adapter adapter = (Adapter)clazz.newInstance();

            Connection connection = new Connection();
            connection.init(connectionConfig, adapter);

            adapter.init(adapterConfig, connection);

            connections.put(connectionConfig.getConnectionName(), connection);

            Collection sourceDefinitions = connectionConfig.getSourceDefinitions();
            for (Iterator k=sourceDefinitions.iterator(); k.hasNext(); ) {
                SourceDefinition sourceDefinition = (SourceDefinition)k.next();

                String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();

                CacheConfig fiterCacheConfig = connectorConfig.getCacheConfig(ConnectorConfig.QUERY_CACHE);
                String filterCacheClass = fiterCacheConfig.getCacheClass();
                filterCacheClass = filterCacheClass == null ? CacheConfig.DEFAULT_CONNECTOR_QUERY_CACHE : filterCacheClass;

                clazz = Class.forName(filterCacheClass);
                ConnectorQueryCache connectorQueryCache = (ConnectorQueryCache)clazz.newInstance();
                connectorQueryCache.setSourceDefinition(sourceDefinition);
                connectorQueryCache.init(fiterCacheConfig);

                sourceFilterCaches.put(key, connectorQueryCache);

                CacheConfig dataCacheConfig = connectorConfig.getCacheConfig(ConnectorConfig.DATA_CACHE);
                String dataCacheClass = dataCacheConfig.getCacheClass();
                dataCacheClass = dataCacheClass == null ? CacheConfig.DEFAULT_CONNECTOR_DATA_CACHE : dataCacheClass;

                clazz = Class.forName(dataCacheClass);
                ConnectorDataCache connectorDataCache = (ConnectorDataCache)clazz.newInstance();
                connectorDataCache.setSourceDefinition(sourceDefinition);
                connectorDataCache.init(dataCacheConfig);

                sourceDataCaches.put(key, connectorDataCache);
            }
        }
    }

    public Connection getConnection(String name) {
        return (Connection)connections.get(name);
    }

    public Config getConfig(SourceDefinition sourceDefinition) throws Exception {
        String connectionName = sourceDefinition.getConnectionName();
        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            if (config.getConnectionConfig(connectionName) != null) return config;
        }
        return null;
    }

    public Collection getConfigs() {
        return configs;
    }

    public void refresh(Config config) throws Exception {

        //log.debug("Refreshing cache ...");

        Collection connectionConfigs = config.getConnectionConfigs();
        for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)j.next();

            Collection sourceDefinitions = connectionConfig.getSourceDefinitions();
            for (Iterator k=sourceDefinitions.iterator(); k.hasNext(); ) {
                SourceDefinition sourceDefinition = (SourceDefinition)k.next();

                String s = sourceDefinition.getParameter(SourceDefinition.AUTO_REFRESH);
                boolean autoRefresh = s == null ? SourceDefinition.DEFAULT_AUTO_REFRESH : new Boolean(s).booleanValue();

                if (!autoRefresh) continue;

                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Refreshing source caches for "+sourceDefinition.getConnectionName()+"/"+sourceDefinition.getName(), 80));
                log.debug(Formatter.displaySeparator(80));

                s = sourceDefinition.getParameter(SourceDefinition.REFRESH_METHOD);
                String refreshMethod = s == null ? SourceDefinition.DEFAULT_REFRESH_METHOD : s;

                if (SourceDefinition.POLL_CHANGES.equals(refreshMethod)) {
                    pollChanges(connectionConfig, sourceDefinition);

                } else { // if (SourceDefinition.RELOAD_EXPIRED.equals(refreshMethod)) {
                    reloadExpired(connectionConfig, sourceDefinition);
                }
            }
        }
    }

    public Row normalize(Row row) throws Exception {

        Row newRow = new Row();

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

    public void pollChanges(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {

        int lastChangeNumber = getSourceDataCache(connectionConfig, sourceDefinition).getLastChangeNumber();

        Connection connection = getConnection(sourceDefinition.getConnectionName());
        SearchResults sr = connection.getChanges(sourceDefinition, lastChangeNumber);
        if (!sr.hasNext()) return;

        Collection pks = new HashSet();

        log.debug("Synchronizing changes:");
        while (sr.hasNext()) {
            Row pk = (Row)sr.next();

            Integer changeNumber = (Integer)pk.remove("changeNumber");
            Object changeTime = pk.remove("changeTime");
            Object changeAction = pk.remove("changeAction");
            Row key = normalize((Row)pk);

            log.debug(" - "+key+": "+changeAction);

            getSourceDataCache(connectionConfig, sourceDefinition).remove(key);

            if ("DELETE".equals(changeAction)) {
                pks.remove(pk);
            } else {
                pks.add(pk);
            }

            lastChangeNumber = changeNumber.intValue();
        }

        getSourceDataCache(connectionConfig, sourceDefinition).setLastChangeNumber(lastChangeNumber);

        load(sourceDefinition, pks);
    }

    public void reloadExpired(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {

        Map map = getSourceDataCache(connectionConfig, sourceDefinition).getExpired();

        log.debug("Reloading expired caches...");

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+av);
        }

        Filter f = FilterTool.createFilter(map.keySet());
        if (f == null) return;

        load(sourceDefinition, map.keySet());
    }

    public synchronized MRSWLock getLock(SourceDefinition sourceDefinition) {
		String name = sourceDefinition.getConnectionName() + "." + sourceDefinition.getName();

		MRSWLock lock = (MRSWLock)locks.get(name);

		if (lock == null) lock = new MRSWLock(queue);
		locks.put(name, lock);

		return lock;
	}

    public int bind(SourceDefinition sourceDefinition, EntryDefinition entry, AttributeValues sourceValues, String password) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Binding as entry in "+sourceDefinition.getName());

        MRSWLock lock = getLock(sourceDefinition);
        lock.getReadLock(WAIT_TIMEOUT);

        try {
            Connection connection = getConnection(sourceDefinition.getConnectionName());
            int rc = connection.bind(sourceDefinition, sourceValues, password);

            return rc;

        } finally {
        	lock.releaseReadLock(WAIT_TIMEOUT);
        }
    }

    public int add(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+sourceDefinition.getName());
        log.debug("Values: "+sourceValues);

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection pks = TransformEngine.getPrimaryKeys(sourceDefinition, sourceValues);

            // Add rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues)sourceValues.clone();
                newEntry.set(pk);
                log.debug("ADDING ROW: " + newEntry);

                // Add row to source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());
                int rc = connection.add(sourceDefinition, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;
            }

            getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
        	lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Deleting entry in "+sourceDefinition.getName());

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection pks = TransformEngine.getPrimaryKeys(sourceDefinition, sourceValues);

            // Remove rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                Row key = normalize((Row)pk);
                AttributeValues oldEntry = (AttributeValues)sourceValues.clone();
                oldEntry.set(pk);
                //log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());
                int rc = connection.delete(sourceDefinition, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                getSourceDataCache(connectionConfig, sourceDefinition).remove(key);
            }

            getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(
            SourceDefinition sourceDefinition,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Modifying entry in " + sourceDefinition.getName());
        //log.debug("Old values: " + oldSourceValues);
        //log.debug("New values: " + newSourceValues);

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection oldPKs = TransformEngine.getPrimaryKeys(sourceDefinition, oldSourceValues);
            Collection newPKs = TransformEngine.getPrimaryKeys(sourceDefinition, newSourceValues);

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Set addRows = new HashSet(newPKs);
            addRows.removeAll(oldPKs);
            log.debug("PKs to add: " + addRows);

            Set removeRows = new HashSet(oldPKs);
            removeRows.removeAll(newPKs);
            log.debug("PKs to remove: " + removeRows);

            Set replaceRows = new HashSet(oldPKs);
            replaceRows.retainAll(newPKs);
            log.debug("PKs to replace: " + replaceRows);

            // Add rows
            for (Iterator i = addRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues)newSourceValues.clone();
                newEntry.set(pk);
                //log.debug("ADDING ROW: " + newEntry);

                // Add row to source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());
                int rc = connection.add(sourceDefinition, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;
            }

            // Remove rows
            for (Iterator i = removeRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                Row key = normalize((Row)pk);
                AttributeValues oldEntry = (AttributeValues)oldSourceValues.clone();
                oldEntry.set(pk);
                //log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());
                int rc = connection.delete(sourceDefinition, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                getSourceDataCache(connectionConfig, sourceDefinition).remove(key);
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
                Connection connection = getConnection(sourceDefinition.getConnectionName());
                int rc = connection.modify(sourceDefinition, oldEntry, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                getSourceDataCache(connectionConfig, sourceDefinition).remove(key);
            }

            getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modrdn(
            SourceDefinition sourceDefinition,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Renaming entry in " + sourceDefinition.getName());

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection oldPKs = TransformEngine.getPrimaryKeys(sourceDefinition, oldSourceValues);
            Collection newPKs = TransformEngine.getPrimaryKeys(sourceDefinition, newSourceValues);

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Iterator i = oldPKs.iterator();
            Iterator j = newPKs.iterator();

            while (i.hasNext() && j.hasNext()) {
                Row oldPk = (Row)i.next();
                Row key = normalize((Row)oldPk);
                Row newPk = (Row)j.next();

                // Rename row from source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());

                int rc;
                if (oldPk.equals(newPk)) {
                    rc = connection.modify(sourceDefinition, oldSourceValues, newSourceValues);
                } else {
                    rc = connection.modrdn(sourceDefinition, oldPk, newPk);
                }

                if (rc != LDAPException.SUCCESS) return rc;

                getSourceDataCache(connectionConfig, sourceDefinition).remove(key);
            }

            getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public Collection search(
            SourceDefinition sourceDefinition,
            Filter filter)
            throws Exception {

        // log.debug("Searching source "+source.getName()+" with filter "+filter);

        Collection results = new ArrayList();

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        Collection uniqueFieldDefinitions = sourceDefinition.getUniqueFieldDefinitions();

        log.debug("Checking source filter cache for "+filter);
        Collection pks = getSourceFilterCache(connectionConfig, sourceDefinition).get(filter);

        if (pks != null) {
            log.debug("Source filter cache found: "+pks);
            //log.debug("Loading source "+source.getName()+" with pks "+pks);
            results.addAll(load(sourceDefinition, pks));
            return results;
        }

        log.debug("Source filter cache not found.");

        String method = sourceDefinition.getParameter(SourceDefinition.LOADING_METHOD);
        //log.debug("Loading method: "+method);

        if (SourceDefinition.SEARCH_AND_LOAD.equals(method)) {
            log.debug("Searching source "+sourceDefinition.getName()+" with filter "+filter);
            SearchResults sr = searchEntries(sourceDefinition, filter);
            pks = new ArrayList();
            pks.addAll(sr.getAll());

            log.debug("Loading source "+sourceDefinition.getName()+" with pks "+pks);
            results.addAll(load(sourceDefinition, pks));

        } else {
            log.debug("Loading source "+sourceDefinition.getName()+" with filter "+filter);
            Map map = loadEntries(sourceDefinition, filter);
            pks = new TreeSet();
            pks.addAll(map.keySet());
            results.addAll(map.values());

            Collection uniqueKeys = new TreeSet();

            for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                Row npk = normalize(pk);
                AttributeValues values = (AttributeValues)map.get(pk);
                getSourceDataCache(connectionConfig, sourceDefinition).put(npk, values);

                Filter f = FilterTool.createFilter(npk);
                Collection list = new TreeSet();
                list.add(npk);

                log.debug("Storing source filter cache "+f+": "+list);
                getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);

                //log.debug("Unique fields:");
                for (Iterator j=uniqueFieldDefinitions.iterator(); j.hasNext(); ) {
                    FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                    Object value = values.getOne(fieldDefinition.getName());

                    Row uniqueKey = new Row();
                    uniqueKey.set(fieldDefinition.getName(), value);

                    //f = connectorContext.getFilterTool().createFilter(uniqueKey);
                    //list = new TreeSet();
                    //list.add(npk);

                    //log.debug(" - "+f+" => "+list);
                    //connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);

                    uniqueKeys.add(uniqueKey);
                }
/*
                log.debug("Indexed fields:");
                for (Iterator j=indexedFieldDefinitions.iterator(); j.hasNext(); ) {
                    FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                    Collection v = values.get(fieldDefinition.getName());

                    for (Iterator k=v.iterator(); k.hasNext(); ) {
                        Object value = k.next();

                        Row indexKey = new Row();
                        indexKey.set(fieldDefinition.getName(), value);

                        f = connectorContextntextntext.getFilterTool().createFilter(indexKey);

                        list = connectorContextntextntext.getSourceFilterCache(connectionConfig, sourceDefinition).get(f);
                        if (list == null) list = new TreeSet();
                        list.add(npk);

                        log.debug("Storing source filter cache "+f+": "+list);
                        connectorContextntextntext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);
                    }
                }
*/
            }

            if (!uniqueKeys.isEmpty()) {
                Filter f = FilterTool.createFilter(uniqueKeys);
                log.debug("Storing source filter cache "+f+": "+pks);
                getSourceFilterCache(connectionConfig, sourceDefinition).put(f, pks);
            }
        }

        log.debug("Storing source filter cache "+filter+": "+pks);
        getSourceFilterCache(connectionConfig, sourceDefinition).put(filter, pks);

        Filter newFilter = FilterTool.createFilter(pks);
        if (newFilter != null) {
            log.debug("Storing source filter cache "+newFilter+": "+pks);
            getSourceFilterCache(connectionConfig, sourceDefinition).put(newFilter, pks);
        }

/*
        log.debug("Checking source cache for pks "+pks);
        Map loadedRows = connectorContextntextntextntext.getSourceDataCache(connectionConfig, sourceDefinition).search(pks);
        log.debug("Loaded rows: "+loadedRows.keySet());
        results.putAll(loadedRows);

        Collection pksToLoad = new HashSet();
        pksToLoad.addAll(pks);
        pksToLoad.removeAll(results.keySet());
        pksToLoad.removeAll(loadedRows.keySet());

        log.debug("PKs to load: "+pksToLoad);
        if (!pksToLoad.isEmpty()) {
            Filter newFilter = connectorContextntextntextntext.getFilterTool().createFilter(pksToLoad);
            Map map = loadEntries(source, newFilter);
            results.putAll(map);

            for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                AttributeValues values = (AttributeValues)map.get(pk);
                connectorContextntextntextntext.getSourceDataCache(connectionConfig, sourceDefinition).put(pk, values);
            }

            connectorContextntextntextntext.getSourceFilterCache(connectionConfig, sourceDefinition).put(newFilter, map.keySet());
        }
*/
        return results;
    }

    public Collection load(
            SourceDefinition sourceDefinition,
            Collection keys)
            throws Exception {

        //log.debug("Loading source "+source.getName()+" with keys "+keys);

        Map results = new TreeMap();

        if (keys.isEmpty()) return results.values();

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        Collection uniqueFieldDefinitions = sourceDefinition.getUniqueFieldDefinitions();
        Collection missingKeys = new ArrayList();

        Collection normalizedKeys = new ArrayList();
        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();
            Row normalizedKey = normalize(key);
            normalizedKeys.add(normalizedKey);
        }

        log.debug("Searching source data cache for "+normalizedKeys);
        Map loadedRows = getSourceDataCache(connectionConfig, sourceDefinition).search(normalizedKeys, missingKeys);
        results.putAll(loadedRows);

        log.debug("Loaded rows: "+loadedRows.keySet());
        log.debug("Missing keys: "+missingKeys);

        Collection keysToLoad = new TreeSet();
        keysToLoad.addAll(missingKeys);
        //keysToLoad.removeAll(results.keySet());
        //keysToLoad.removeAll(loadedRows.keySet());

        log.debug("PKs to load: "+keysToLoad);
        if (!keysToLoad.isEmpty()) {
            Filter newFilter = FilterTool.createFilter(keysToLoad);
            Map map = loadEntries(sourceDefinition, newFilter);
            results.putAll(map);

            Collection uniqueKeys = new TreeSet();

            for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                Row npk = normalize(pk);
                AttributeValues values = (AttributeValues)map.get(pk);

                getSourceDataCache(connectionConfig, sourceDefinition).put(npk, values);

                Filter f = FilterTool.createFilter(npk);
                Collection list = new TreeSet();
                list.add(npk);

                log.debug("Storing source filter cache "+f+": "+list);
                getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);

                //log.debug("Unique fields:");
                for (Iterator j=uniqueFieldDefinitions.iterator(); j.hasNext(); ) {
                    FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                    Object value = values.getOne(fieldDefinition.getName());

                    Row uniqueKey = new Row();
                    uniqueKey.set(fieldDefinition.getName(), value);

                    //f = connectorContext.getFilterTool().createFilter(normalizedUniqueKey);
                    //list = new TreeSet();
                    //list.add(npk);

                    //log.debug(" - "+f+" => "+list);
                    //connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);

                    uniqueKeys.add(uniqueKey);
                }
/*
                log.debug("Indexed fields:");
                for (Iterator j=indexedFieldDefinitions.iterator(); j.hasNext(); ) {
                    FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                    Collection v = values.get(fieldDefinition.getName());

                    for (Iterator k=v.iterator(); k.hasNext(); ) {
                        Object value = k.next();

                        Row indexKey = new Row();
                        indexKey.set(fieldDefinition.getName(), value);
                        Row normalizedIndexKey = connectorContextntextntextntext.getSchema().normalize(indexKey);

                        f = connectorContextntextntextntext.getFilterTool().createFilter(normalizedIndexKey);

                        list = connectorContextntextntextntext.getSourceFilterCache(connectionConfig, sourceDefinition).get(f);
                        if (list == null) list = new TreeSet();
                        list.add(npk);

                        log.debug("Storing source filter cache "+f+": "+list);
                        connectorContextntextntextntext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);
                    }
                }
*/
            }

            if (!uniqueKeys.isEmpty()) {
                Filter f = FilterTool.createFilter(uniqueKeys);
                log.debug("Storing source filter cache "+f+": "+keys);
                getSourceFilterCache(connectionConfig, sourceDefinition).put(f, keys);
            }

            Collection list = new TreeSet();
            list.addAll(map.keySet());
            log.debug("Storing source filter cache "+newFilter+": "+list);
            getSourceFilterCache(connectionConfig, sourceDefinition).put(newFilter, list);
        }

        return results.values();
    }

    public SearchResults searchEntries(SourceDefinition sourceDefinition, Filter filter) throws Exception {

        SearchResults results = new SearchResults();

        String s = sourceDefinition.getParameter(SourceDefinition.SIZE_LIMIT);
        int sizeLimit = s == null ? SourceDefinition.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        Connection connection = getConnection(sourceDefinition.getConnectionName());
        SearchResults sr;
        try {
            sr = connection.search(sourceDefinition, filter, sizeLimit);

        } catch (Exception e) {
            e.printStackTrace();
            results.close();
            return results;
        }

        //log.debug("Search results:");

        for (Iterator i=sr.iterator(); i.hasNext();) {
            Row pk = (Row)i.next();

            Row npk = normalize(pk);
            //log.debug(" - PK: "+npk);

            results.add(npk);
        }

        results.setReturnCode(sr.getReturnCode());
        results.close();

        return results;
    }

    public void load(SourceDefinition sourceDefinition) throws Exception {

        String s = sourceDefinition.getParameter(SourceDefinition.SIZE_LIMIT);
        int sizeLimit = s == null ? SourceDefinition.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        Connection connection = getConnection(sourceDefinition.getConnectionName());

        SearchResults sr;
        try {
            sr = connection.load(sourceDefinition, null, sizeLimit);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }

    public Map loadEntries(SourceDefinition sourceDefinition, Filter filter) throws Exception {

        Map results = new TreeMap();

        String s = sourceDefinition.getParameter(SourceDefinition.SIZE_LIMIT);
        int sizeLimit = s == null ? SourceDefinition.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        Connection connection = getConnection(sourceDefinition.getConnectionName());
        SearchResults sr;
        try {
            sr = connection.load(sourceDefinition, filter, sizeLimit);
        } catch (Exception e) {
            e.printStackTrace();
            return results;
        }

        //log.debug("Load results:");

        for (Iterator i=sr.iterator(); i.hasNext();) {
            AttributeValues sourceValues = (AttributeValues)i.next();

            Row pk = sourceDefinition.getPrimaryKeyValues(sourceValues);
            if (pk == null) continue;

            Row npk = normalize(pk);
            //log.debug(" - PK: "+npk);

            results.put(npk, sourceValues);
        }

        return results;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectorQueryCache getSourceFilterCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        return (ConnectorQueryCache)sourceFilterCaches.get(key);
    }

    public ConnectorDataCache getSourceDataCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        return (ConnectorDataCache)sourceDataCaches.get(key);
    }

    public static void load(Adapter adapter, ConnectorDataCache cache, SourceDefinition srcDef) throws Exception {
        String s = srcDef.getParameter(SourceDefinition.AUTO_REFRESH);
        boolean autoRefresh = s == null ? SourceDefinition.DEFAULT_AUTO_REFRESH : new Boolean(s).booleanValue();

        if (!autoRefresh) return;

        SearchResults sr = adapter.load(srcDef, null, 100);

        //log.debug("Results:");
        while (sr.hasNext()) {
            AttributeValues sourceValues = (AttributeValues)sr.next();
            Row pk = srcDef.getPrimaryKeyValues(sourceValues);
            //log.debug(" - "+pk+": "+sourceValues);

            cache.put(pk, sourceValues);
        }

        int lastChangeNumber = adapter.getLastChangeNumber(srcDef);
        cache.setLastChangeNumber(lastChangeNumber);
    }

    public static void main(String args[]) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: org.safehaus.penrose.connector.Connector [command]");
            System.out.println();
            System.out.println("Commands:");
            System.out.println("    create - create connector caches");
            System.out.println("    load   - load data from data sources");
            System.out.println("    clean  - clean connector caches");
            System.exit(0);
        }

        String homeDirectory = System.getProperty("penrose.home");
        log.debug("PENROSE_HOME: "+homeDirectory);

        String command = args[0];

        ServerConfigReader serverConfigReader = new ServerConfigReader();
        ServerConfig serverCfg = serverConfigReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");

        ConnectorConfig connectorCfg = serverCfg.getConnectorConfig();

        Connector connector = new Connector();
        connector.init(serverCfg, connectorCfg);

        ConfigReader configReader = new ConfigReader();
        Config config = configReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        addConfig(connector, config, command);

        File partitions = new File(homeDirectory+File.separator+"partitions");
        if (partitions.exists()) {
            File files[] = partitions.listFiles();
            for (int i=0; i<files.length; i++) {
                File partition = files[i];
                String name = partition.getName();

                config = configReader.read(partition.getAbsolutePath());
                addConfig(connector, config, command);
            }
        }

        if ("run".equals(command)) {
            connector.start();
        }

    }

    public static void addConfig(Connector connector, Config config, String command) throws Exception {
        try {
            connector.addConfig(config);

            Collection connectionConfigs = config.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig conCfg = (ConnectionConfig)j.next();

                Connection connection = connector.getConnection(conCfg.getConnectionName());
                Adapter adapter = connection.getAdapter();

                Collection sourceDefinitions = conCfg.getSourceDefinitions();
                for (Iterator k=sourceDefinitions.iterator(); k.hasNext(); ) {
                    SourceDefinition srcDef = (SourceDefinition)k.next();

                    ConnectorDataCache connectorDataCache = connector.getSourceDataCache(conCfg, srcDef);

                    if ("create".equals(command)) {
                        connectorDataCache.create();

                    } else if ("load".equals(command)) {
                        load(adapter, connectorDataCache, srcDef);

                    } else if ("clean".equals(command)) {
                        connectorDataCache.clean();
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
