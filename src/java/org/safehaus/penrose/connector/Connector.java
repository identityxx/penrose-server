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

        int lastChangeNumber = getDataCache(connectionConfig, sourceDefinition).getLastChangeNumber();

        Connection connection = getConnection(sourceDefinition.getConnectionName());
        SearchResults sr = connection.getChanges(sourceDefinition, lastChangeNumber);
        if (!sr.hasNext()) return;

        CacheConfig dataCacheConfig = connectorConfig.getCacheConfig(ConnectorConfig.DATA_CACHE);
        String user = dataCacheConfig.getParameter("user");

        Collection pks = new HashSet();

        log.debug("Synchronizing changes:");
        while (sr.hasNext()) {
            Row pk = (Row)sr.next();

            Integer changeNumber = (Integer)pk.remove("changeNumber");
            Object changeTime = pk.remove("changeTime");
            String changeAction = (String)pk.remove("changeAction");
            String changeUser = (String)pk.remove("changeUser");
            Row key = normalize((Row)pk);

            log.debug(" - "+key+": "+changeAction);

            lastChangeNumber = changeNumber.intValue();

            if (user != null && user.equals(changeUser)) continue;

            if ("DELETE".equals(changeAction)) {
                pks.remove(pk);
            } else {
                pks.add(pk);
            }

            getDataCache(connectionConfig, sourceDefinition).remove(key);
        }

        getDataCache(connectionConfig, sourceDefinition).setLastChangeNumber(lastChangeNumber);

        retrieve(connectionConfig, sourceDefinition, pks);
    }

    public void reloadExpired(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {

        Map map = getDataCache(connectionConfig, sourceDefinition).getExpired();

        log.debug("Reloading expired caches...");

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+av);
        }

        retrieve(connectionConfig, sourceDefinition, map.keySet());
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
        lock.getReadLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Connection connection = getConnection(sourceDefinition.getConnectionName());
            int rc = connection.bind(sourceDefinition, sourceValues, password);

            return rc;

        } finally {
        	lock.releaseReadLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }
    }

    public int add(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+sourceDefinition.getName());
        log.debug("Values: "+sourceValues);

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

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

            retrieve(connectionConfig, sourceDefinition, pks);
            //getQueryCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
        	lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Deleting entry in "+sourceDefinition.getName());

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Collection pks = TransformEngine.getPrimaryKeys(sourceDefinition, sourceValues);

            // Remove rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                Row key = normalize((Row)pk);
                AttributeValues oldEntry = (AttributeValues)sourceValues.clone();
                oldEntry.set(pk);
                log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());
                int rc = connection.delete(sourceDefinition, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                getDataCache(connectionConfig, sourceDefinition).remove(key);
            }

            //getQueryCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
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
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

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

            Collection pks = new ArrayList();

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

                pks.add(pk);
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
                getDataCache(connectionConfig, sourceDefinition).remove(key);
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
                getDataCache(connectionConfig, sourceDefinition).remove(key);
                pks.add(pk);
            }

            retrieve(connectionConfig, sourceDefinition, pks);

            //getQueryCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
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
        lock.getWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);

        try {
            Collection oldPKs = TransformEngine.getPrimaryKeys(sourceDefinition, oldSourceValues);
            Collection newPKs = TransformEngine.getPrimaryKeys(sourceDefinition, newSourceValues);

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Iterator i = oldPKs.iterator();
            Iterator j = newPKs.iterator();

            while (i.hasNext() && j.hasNext()) {
                Row oldPk = (Row)i.next();
                Row newPk = (Row)j.next();

                Row oldKey = normalize((Row)oldPk);
                Row newKey = normalize((Row)newPk);

                // Rename row from source table in the source database/directory
                Connection connection = getConnection(sourceDefinition.getConnectionName());

                int rc;
                if (oldPk.equals(newPk)) {
                    rc = connection.modify(sourceDefinition, oldSourceValues, newSourceValues);
                } else {
                    rc = connection.modrdn(sourceDefinition, oldPk, newPk);
                }

                if (rc != LDAPException.SUCCESS) return rc;

                getDataCache(connectionConfig, sourceDefinition).remove(oldKey);
            }

            retrieve(connectionConfig, sourceDefinition, newPKs);

            //getQueryCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    /**
     * Search the data sources.
     */
    public Collection search(
            SourceDefinition sourceDefinition,
            Filter filter)
            throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        String method = sourceDefinition.getParameter(SourceDefinition.LOADING_METHOD);
        if (SourceDefinition.SEARCH_AND_LOAD.equals(method)) { // search for PKs first then load full record

            log.debug("Searching source "+sourceDefinition.getName()+" with filter "+filter);
            Collection pks = search(connectionConfig, sourceDefinition, filter);

            log.debug("Loading source "+sourceDefinition.getName()+" with pks "+pks);
            return load(connectionConfig, sourceDefinition, pks);

        } else { // load full record immediately

            log.debug("Loading source "+sourceDefinition.getName()+" with filter "+filter);
            return load(connectionConfig, sourceDefinition, filter);
        }
    }

    /**
     * Check query cache, peroform search, store results in query cache.
     */
    public Collection search(
            ConnectionConfig connectionConfig,
            SourceDefinition sourceDefinition,
            Filter filter)
            throws Exception {

        log.debug("Checking query cache for "+filter);
        Collection pks = getQueryCache(connectionConfig, sourceDefinition).search(filter);

        log.debug("Cached results: "+pks);
        if (pks != null) return pks;

        log.debug("Searching source "+sourceDefinition.getName()+" with filter "+filter);
        pks = performSearch(sourceDefinition, filter);

        log.debug("Storing query cache for "+filter);
        getQueryCache(connectionConfig, sourceDefinition).put(filter, pks);

        return pks;
    }

    /**
     * Check data cache then load.
     */
    public Collection load(
            ConnectionConfig connectionConfig,
            SourceDefinition sourceDefinition,
            Collection pks)
            throws Exception {

        Collection results = new ArrayList();
        if (pks.isEmpty()) return results;

        Collection normalizedPks = new ArrayList();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            Row npk = normalize(pk);
            normalizedPks.add(npk);
        }

        log.debug("Checking data cache for "+normalizedPks);
        Collection missingPks = new ArrayList();
        Map loadedRows = getDataCache(connectionConfig, sourceDefinition).load(normalizedPks, missingPks);

        log.debug("Cached values: "+loadedRows.keySet());
        results.addAll(loadedRows.values());

        log.debug("Loading missing keys: "+missingPks);
        Collection list = retrieve(connectionConfig, sourceDefinition, missingPks);
        results.addAll(list);

        return results;
    }

    /**
     * Load then store in data cache.
     */
    public Collection load(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition, Filter filter) throws Exception {

        Collection pks = getQueryCache(connectionConfig, sourceDefinition).search(filter);

        if (pks != null) {
            return load(connectionConfig, sourceDefinition, pks);
        }

        Collection values = performLoad(sourceDefinition, filter);
        store(connectionConfig, sourceDefinition, values);

        return values;
    }

    /**
     * Load then store in data cache.
     */
    public Collection retrieve(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition, Collection keys) throws Exception {

        if (keys.isEmpty()) return new ArrayList();

        Filter filter = FilterTool.createFilter(keys);
        Collection values = performLoad(sourceDefinition, filter);

        store(connectionConfig, sourceDefinition, values);

        return values;
    }

    public void store(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition, Collection values) throws Exception {

        Collection pks = new TreeSet();

        Collection uniqueFieldDefinitions = sourceDefinition.getUniqueFieldDefinitions();
        Collection uniqueKeys = new TreeSet();

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            Row pk = sourceDefinition.getPrimaryKeyValues(sourceValues);
            Row npk = normalize(pk);

            pks.add(npk);

            log.debug("Storing data cache: "+npk);
            getDataCache(connectionConfig, sourceDefinition).put(npk, sourceValues);

            Filter f = FilterTool.createFilter(npk);
            Collection c = new TreeSet();
            c.add(npk);

            log.debug("Storing query cache "+f+": "+c);
            getQueryCache(connectionConfig, sourceDefinition).put(f, c);

            for (Iterator j=uniqueFieldDefinitions.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                String fieldName = fieldDefinition.getName();

                Object value = sourceValues.getOne(fieldName);

                Row uniqueKey = new Row();
                uniqueKey.set(fieldName, value);

                uniqueKeys.add(uniqueKey);
            }
        }

        if (!uniqueKeys.isEmpty()) {
            Filter f = FilterTool.createFilter(uniqueKeys);
            log.debug("Storing query cache "+f+": "+pks);
            getQueryCache(connectionConfig, sourceDefinition).put(f, pks);
        }

        Filter filter = FilterTool.createFilter(pks);
        log.debug("Storing query cache "+filter+": "+pks);
        getQueryCache(connectionConfig, sourceDefinition).put(filter, pks);
    }

    /**
     * Perform the search operation.
     */
    public Collection performSearch(SourceDefinition sourceDefinition, Filter filter) throws Exception {

        Collection results = new ArrayList();

        String s = sourceDefinition.getParameter(SourceDefinition.SIZE_LIMIT);
        int sizeLimit = s == null ? SourceDefinition.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        Connection connection = getConnection(sourceDefinition.getConnectionName());
        SearchResults sr;
        try {
            sr = connection.search(sourceDefinition, filter, sizeLimit);

        } catch (Exception e) {
            e.printStackTrace();
            return results;
        }

        for (Iterator i=sr.iterator(); i.hasNext();) {
            Row pk = (Row)i.next();
            Row npk = normalize(pk);
            results.add(npk);
        }

        return results;
    }

    /**
     * Perform the load operation.
     */
    public Collection performLoad(SourceDefinition sourceDefinition, Filter filter) throws Exception {

        Collection results = new ArrayList();

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

        for (Iterator i=sr.iterator(); i.hasNext();) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            results.add(sourceValues);
        }

        return results;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectorDataCache getQueryCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        return (ConnectorDataCache)sourceDataCaches.get(key);
    }

    public ConnectorDataCache getDataCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
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

                    ConnectorDataCache connectorDataCache = connector.getDataCache(conCfg, srcDef);

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
