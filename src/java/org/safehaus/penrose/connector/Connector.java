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
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.ConnectorCache;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.config.ServerConfigReader;
import org.safehaus.penrose.config.ConfigReader;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.ThreadPool;
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

    private Map caches = new TreeMap();

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

                CacheConfig dataCacheConfig = connectorConfig.getCacheConfig(ConnectorConfig.CACHE);
                String dataCacheClass = dataCacheConfig.getCacheClass();
                dataCacheClass = dataCacheClass == null ? CacheConfig.DEFAULT_CONNECTOR_CACHE : dataCacheClass;

                clazz = Class.forName(dataCacheClass);
                ConnectorCache connectorCache = (ConnectorCache)clazz.newInstance();
                connectorCache.setConnector(this);
                connectorCache.setSourceDefinition(sourceDefinition);
                connectorCache.init(dataCacheConfig);

                caches.put(key, connectorCache);
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

                if (log.isDebugEnabled()) {
                    log.debug(Formatter.displaySeparator(80));
                    log.debug(Formatter.displayLine("Refreshing source caches for "+sourceDefinition.getConnectionName()+"/"+sourceDefinition.getName(), 80));
                    log.debug(Formatter.displaySeparator(80));
                }

                s = sourceDefinition.getParameter(SourceDefinition.REFRESH_METHOD);
                String refreshMethod = s == null ? SourceDefinition.DEFAULT_REFRESH_METHOD : s;

                if (SourceDefinition.POLL_CHANGES.equals(refreshMethod)) {
                    pollChanges(sourceDefinition);

                } else { // if (SourceDefinition.RELOAD_EXPIRED.equals(refreshMethod)) {
                    reloadExpired(sourceDefinition);
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

    public void pollChanges(SourceDefinition sourceDefinition) throws Exception {

        int lastChangeNumber = getCache(sourceDefinition).getLastChangeNumber();

        Connection connection = getConnection(sourceDefinition.getConnectionName());
        SearchResults sr = connection.getChanges(sourceDefinition, lastChangeNumber);
        if (!sr.hasNext()) return;

        CacheConfig dataCacheConfig = connectorConfig.getCacheConfig(ConnectorConfig.CACHE);
        String user = dataCacheConfig.getParameter("user");

        Collection pks = new HashSet();

        log.debug("Synchronizing changes:");
        while (sr.hasNext()) {
            Row pk = (Row)sr.next();

            Integer changeNumber = (Integer)pk.remove("changeNumber");
            Object changeTime = pk.remove("changeTime");
            String changeAction = (String)pk.remove("changeAction");
            String changeUser = (String)pk.remove("changeUser");

            log.debug(" - "+pk+": "+changeAction);

            lastChangeNumber = changeNumber.intValue();

            if (user != null && user.equals(changeUser)) continue;

            if ("DELETE".equals(changeAction)) {
                pks.remove(pk);
            } else {
                pks.add(pk);
            }

            getCache(sourceDefinition).remove(pk);
        }

        getCache(sourceDefinition).setLastChangeNumber(lastChangeNumber);

        retrieve(sourceDefinition, pks);
    }

    public void reloadExpired(SourceDefinition sourceDefinition) throws Exception {

        Map map = getCache(sourceDefinition).getExpired();

        log.debug("Reloading expired caches...");

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+av);
        }

        retrieve(sourceDefinition, map.keySet());
    }

    public void create() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            ConnectorCache connectorCache = (ConnectorCache)i.next();
            connectorCache.create();
        }
    }

    public void load() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            ConnectorCache connectorCache = (ConnectorCache)i.next();
            connectorCache.load();
        }
    }

    public void clean() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            ConnectorCache connectorCache = (ConnectorCache)i.next();
            connectorCache.clean();
        }
    }

    public void drop() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            ConnectorCache connectorCache = (ConnectorCache)i.next();
            connectorCache.drop();
        }
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

            retrieve(sourceDefinition, pks);
            //getQueryCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
        	lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

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
                getCache(sourceDefinition).remove(key);
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

        log.debug("Modifying entry in " + sourceDefinition.getName());

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
                getCache(sourceDefinition).remove(key);
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
                getCache(sourceDefinition).remove(key);
                pks.add(pk);
            }

            retrieve(sourceDefinition, pks);

            //getQueryCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(ConnectorConfig.DEFAULT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    /**
     * Search the data sources.
     */
    public SearchResults search(
            final SourceDefinition sourceDefinition,
            final Filter filter)
            throws Exception {

        String method = sourceDefinition.getParameter(SourceDefinition.LOADING_METHOD);
        if (SourceDefinition.SEARCH_AND_LOAD.equals(method)) { // search for PKs first then load full record

            log.debug("Searching source "+sourceDefinition.getName()+" with filter "+filter);
            return searchAndLoad(sourceDefinition, filter);

        } else { // load full record immediately

            log.debug("Loading source "+sourceDefinition.getName()+" with filter "+filter);
            return fullLoad(sourceDefinition, filter);
        }
    }

    /**
     * Check query cache, peroform search, store results in query cache.
     */
    public SearchResults searchAndLoad(
            SourceDefinition sourceDefinition,
            Filter filter)
            throws Exception {

        log.debug("Checking query cache for "+filter);
        Collection results = getCache(sourceDefinition).search(filter);

        log.debug("Cached results: "+results);
        if (results != null) {
            SearchResults sr = new SearchResults();
            sr.addAll(results);
            sr.close();
            return sr;
        }

        log.debug("Searching source "+sourceDefinition.getName()+" with filter "+filter);
        Collection pks = performSearch(sourceDefinition, filter);

        log.debug("Storing query cache for "+filter);
        getCache(sourceDefinition).put(filter, pks);

        log.debug("Loading source "+sourceDefinition.getName()+" with pks "+pks);
        return load(sourceDefinition, pks);

    }

    /**
     * Load then store in data cache.
     */
    public SearchResults fullLoad(SourceDefinition sourceDefinition, Filter filter) throws Exception {

        Collection values = new ArrayList();

        Collection pks = getCache(sourceDefinition).search(filter);

        if (pks != null) {
            return load(sourceDefinition, pks);

        } else {
            return performLoad(sourceDefinition, filter);
            //store(sourceDefinition, values);
        }
    }

    /**
     * Check data cache then load.
     */
    public SearchResults load(
            final SourceDefinition sourceDefinition,
            final Collection pks)
            throws Exception {

        final SearchResults results = new SearchResults();

        if (pks.isEmpty()) {
            results.close();
            return results;
        }

        execute(new Runnable() {
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
                    Map loadedRows = getCache(sourceDefinition).load(normalizedPks, missingPks);

                    log.debug("Cached values: "+loadedRows.keySet());
                    results.addAll(loadedRows.values());

                    log.debug("Loading missing keys: "+missingPks);
                    Collection list = retrieve(sourceDefinition, missingPks);
                    results.addAll(list);

                } catch (Exception e) {
                    e.printStackTrace();
                }

                results.close();
            }
        });

        return results;
    }

    /**
     * Load then store in data cache.
     */
    public Collection retrieve(SourceDefinition sourceDefinition, Collection keys) throws Exception {

        if (keys.isEmpty()) return new ArrayList();

        Filter filter = FilterTool.createFilter(keys);

        SearchResults sr = performLoad(sourceDefinition, filter);

        Collection values = new ArrayList();
        values.addAll(sr.getAll());

        //store(sourceDefinition, values);

        return values;
    }

    public Row store(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {
        Row pk = sourceDefinition.getPrimaryKeyValues(sourceValues);
        Row npk = normalize(pk);

        //log.debug("Storing connector cache: "+pk);
        getCache(sourceDefinition).put(pk, sourceValues);

        Filter f = FilterTool.createFilter(npk);
        Collection c = new TreeSet();
        c.add(npk);

        //log.debug("Storing query cache "+f+": "+c);
        getCache(sourceDefinition).put(f, c);

        return npk;
    }

    public void store(SourceDefinition sourceDefinition, Collection values) throws Exception {

        Collection pks = new TreeSet();

        Collection uniqueFieldDefinitions = sourceDefinition.getUniqueFieldDefinitions();
        Collection uniqueKeys = new TreeSet();

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            Row npk = store(sourceDefinition, sourceValues);
            pks.add(npk);

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
            getCache(sourceDefinition).put(f, pks);
        }

        if (pks.size() <= 10) {
            Filter filter = FilterTool.createFilter(pks);
            log.debug("Storing query cache "+filter+": "+pks);
            getCache(sourceDefinition).put(filter, pks);
        }
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
    public SearchResults performLoad(final SourceDefinition sourceDefinition, final Filter filter) throws Exception {

        final SearchResults results = new SearchResults();

        execute(new Runnable() {
            public void run() {
                String s = sourceDefinition.getParameter(SourceDefinition.SIZE_LIMIT);
                int sizeLimit = s == null ? SourceDefinition.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

                Connection connection = getConnection(sourceDefinition.getConnectionName());
                SearchResults sr;
                try {
                    sr = connection.load(sourceDefinition, filter, sizeLimit);

                    for (Iterator i=sr.iterator(); i.hasNext();) {
                        AttributeValues sourceValues = (AttributeValues)i.next();
                        store(sourceDefinition, sourceValues);
                        results.add(sourceValues);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                results.close();
            }
        });

        return results;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectorCache getCache(SourceDefinition sourceDefinition) throws Exception {

        Config config = getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        return (ConnectorCache)caches.get(key);
    }

    public static void main(String args[]) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: org.safehaus.penrose.connector.Connector [command]");
            System.out.println();
            System.out.println("Commands:");
            System.out.println("    create - create cache tables");
            System.out.println("    load   - load data into cache tables");
            System.out.println("    clean  - clean data from cache tables");
            System.out.println("    drop   - drop cache tables");
            System.exit(0);
        }

        String homeDirectory = System.getProperty("penrose.home");
        log.debug("PENROSE_HOME: "+homeDirectory);

        String command = args[0];

        ServerConfigReader serverConfigReader = new ServerConfigReader();
        ServerConfig serverCfg = serverConfigReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");

        Collection cfgs = getConfigs(homeDirectory);
        createConnector(serverCfg, cfgs, command);
    }

    public static Collection getConfigs(String homeDirectory) throws Exception {
        Collection cfgs = new ArrayList();

        ConfigReader configReader = new ConfigReader();
        Config config = configReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        cfgs.add(config);

        File partitions = new File(homeDirectory+File.separator+"partitions");
        if (partitions.exists()) {
            File files[] = partitions.listFiles();
            for (int i=0; i<files.length; i++) {
                File partition = files[i];
                String name = partition.getName();

                config = configReader.read(partition.getAbsolutePath());
                cfgs.add(config);
            }
        }

        return cfgs;
    }

    public static Connector createConnector(
            ServerConfig serverCfg,
            Collection cfgs,
            String command) throws Exception {

        ConnectorConfig connectorCfg = serverCfg.getConnectorConfig();

        Connector connector = new Connector();
        connector.init(serverCfg, connectorCfg);

        for (Iterator i=cfgs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            connector.addConfig(config);
        }

        if ("create".equals(command)) {
            connector.create();

        } else if ("load".equals(command)) {
            connector.load();

        } else if ("clean".equals(command)) {
            connector.clean();

        } else if ("drop".equals(command)) {
            connector.drop();

        } else if ("run".equals(command)) {
            connector.start();
        }

        return connector;
    }
}
