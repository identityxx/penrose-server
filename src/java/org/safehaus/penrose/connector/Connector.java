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
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Connector {

    public final static int WAIT_TIMEOUT = 10000; // wait timeout is 10 seconds

    Logger log = Logger.getLogger(getClass());

    private ConnectorConfig connectorConfig;
    private ConnectorContext connectorContext;

    private ThreadPool threadPool;
    private boolean stopping = false;

    private Map locks = new HashMap();
    private Queue queue = new Queue();

    public Collection configs = new ArrayList();

    public void init(ConnectorConfig connectorConfig, ConnectorContext connectorContext) throws Exception {
        this.connectorConfig = connectorConfig;
        this.connectorContext = connectorContext;

        log.debug("-------------------------------------------------");
        log.debug("Initializing "+connectorConfig.getConnectorName()+" connector ...");

        String s = connectorConfig.getParameter(ConnectorConfig.THREAD_POOL_SIZE);
        int threadPoolSize = s == null ? ConnectorConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        threadPool = new ThreadPool(threadPoolSize);

        execute(new ReloadThread(this));
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

    public void init(Config config) throws Exception {
        configs.add(config);

        Collection connectionConfigs = config.getConnectionConfigs();
        for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)j.next();

            Collection sourceDefinitions = connectionConfig.getSourceDefinitions();
            for (Iterator k=sourceDefinitions.iterator(); k.hasNext(); ) {
                SourceDefinition sourceDefinition = (SourceDefinition)k.next();

                String s = sourceDefinition.getParameter(SourceDefinition.AUTO_RELOAD);
                boolean autoReload = s == null ? SourceDefinition.DEFAULT_AUTO_RELOAD : new Boolean(s).booleanValue();

                if (!autoReload) continue;

                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Loading source caches for "+sourceDefinition.getConnectionName()+"/"+sourceDefinition.getName(), 80));
                log.debug(Formatter.displaySeparator(80));

                search(sourceDefinition, null);
            }
        }
    }

    public void refresh() throws Exception {
        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();

            Collection connectionConfigs = config.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();

                Collection sourceDefinitions = connectionConfig.getSourceDefinitions();
                for (Iterator k=sourceDefinitions.iterator(); k.hasNext(); ) {
                    SourceDefinition sourceDefinition = (SourceDefinition)k.next();

                    String s = sourceDefinition.getParameter(SourceDefinition.AUTO_RELOAD);
                    boolean autoReload = s == null ? SourceDefinition.DEFAULT_AUTO_RELOAD : new Boolean(s).booleanValue();

                    if (!autoReload) continue;

                    log.debug(Formatter.displaySeparator(80));
                    log.debug(Formatter.displayLine("Refreshing source caches for "+sourceDefinition.getConnectionName()+"/"+sourceDefinition.getName(), 80));
                    log.debug(Formatter.displaySeparator(80));

                    refresh(connectionConfig, sourceDefinition);
                }
            }
        }
    }

    public void refresh(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        Map map = connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).getExpired();

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues av = (AttributeValues)map.get(pk);
            log.debug("Refreshing "+pk+": "+av);
        }

        Filter f = connectorContext.getFilterTool().createFilter(map.keySet());
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
            Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
            int rc = connection.bind(sourceDefinition, sourceValues, password);

            return rc;

        } finally {
        	lock.releaseReadLock(WAIT_TIMEOUT);
        }
    }

    public int add(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Config config = connectorContext.getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+sourceDefinition.getName());
        log.debug("Values: "+sourceValues);

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection pks = connectorContext.getTransformEngine().getPrimaryKeys(sourceDefinition, sourceValues);

            // Add rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues)sourceValues.clone();
                newEntry.set(pk);
                log.debug("ADDING ROW: " + newEntry);

                // Add row to source table in the source database/directory
                Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
                int rc = connection.add(sourceDefinition, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;
            }

            connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
        	lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceDefinition sourceDefinition, AttributeValues sourceValues) throws Exception {

        Config config = connectorContext.getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Deleting entry in "+sourceDefinition.getName());

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection pks = connectorContext.getTransformEngine().getPrimaryKeys(sourceDefinition, sourceValues);

            // Remove rows
            for (Iterator i = pks.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues)sourceValues.clone();
                oldEntry.set(pk);
                //log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
                int rc = connection.delete(sourceDefinition, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).remove(pk);
            }

            connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(
            SourceDefinition sourceDefinition,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues) throws Exception {

        Config config = connectorContext.getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Modifying entry in " + sourceDefinition.getName());
        //log.debug("Old values: " + oldSourceValues);
        //log.debug("New values: " + newSourceValues);

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection oldPKs = connectorContext.getTransformEngine().getPrimaryKeys(sourceDefinition, oldSourceValues);
            Collection newPKs = connectorContext.getTransformEngine().getPrimaryKeys(sourceDefinition, newSourceValues);

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
                Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
                int rc = connection.add(sourceDefinition, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;
            }

            // Remove rows
            for (Iterator i = removeRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues)oldSourceValues.clone();
                oldEntry.set(pk);
                //log.debug("DELETE ROW: " + oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
                int rc = connection.delete(sourceDefinition, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).remove(pk);
            }

            // Replace rows
            for (Iterator i = replaceRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues)oldSourceValues.clone();
                oldEntry.set(pk);
                AttributeValues newEntry = (AttributeValues)newSourceValues.clone();
                newEntry.set(pk);
                //log.debug("REPLACE ROW: " + oldEntry+" with "+newEntry);

                // Modify row from source table in the source database/directory
                Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
                int rc = connection.modify(sourceDefinition, oldEntry, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).remove(pk);
            }

            connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modrdn(
            SourceDefinition sourceDefinition,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues) throws Exception {

        Config config = connectorContext.getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        log.debug("Renaming entry in " + sourceDefinition.getName());

        MRSWLock lock = getLock(sourceDefinition);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {
            Collection oldPKs = connectorContext.getTransformEngine().getPrimaryKeys(sourceDefinition, oldSourceValues);
            Collection newPKs = connectorContext.getTransformEngine().getPrimaryKeys(sourceDefinition, newSourceValues);

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Iterator i = oldPKs.iterator();
            Iterator j = newPKs.iterator();

            while (i.hasNext() && j.hasNext()) {
                Row oldPk = (Row)i.next();
                Row newPk = (Row)j.next();

                // Rename row from source table in the source database/directory
                Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());

                int rc;
                if (oldPk.equals(newPk)) {
                    rc = connection.modify(sourceDefinition, oldSourceValues, newSourceValues);
                } else {
                    rc = connection.modrdn(sourceDefinition, oldPk, newPk);
                }

                if (rc != LDAPException.SUCCESS) return rc;

                connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).remove(oldPk);
            }

            connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).invalidate();

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

        Config config = connectorContext.getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        Collection uniqueFieldDefinitions = sourceDefinition.getUniqueFieldDefinitions();

        log.debug("Checking source filter cache for "+filter);
        Collection pks = connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).get(filter);

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
                Row npk = connectorContext.getSchema().normalize(pk);
                AttributeValues values = (AttributeValues)map.get(pk);
                connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).put(npk, values);

                Filter f = connectorContext.getFilterTool().createFilter(npk);
                Collection list = new TreeSet();
                list.add(npk);

                log.debug("Storing source filter cache "+f+": "+list);
                connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);

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
                Filter f = connectorContext.getFilterTool().createFilter(uniqueKeys);
                log.debug("Storing source filter cache "+f+": "+pks);
                connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, pks);
            }
        }

        log.debug("Storing source filter cache "+filter+": "+pks);
        connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(filter, pks);

        Filter newFilter = connectorContext.getFilterTool().createFilter(pks);
        if (newFilter != null) {
            log.debug("Storing source filter cache "+newFilter+": "+pks);
            connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(newFilter, pks);
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

        Config config = connectorContext.getConfig(sourceDefinition);
        ConnectionConfig connectionConfig = config.getConnectionConfig(sourceDefinition.getConnectionName());

        Collection uniqueFieldDefinitions = sourceDefinition.getUniqueFieldDefinitions();
        Collection missingKeys = new ArrayList();

        //log.debug("Searching source data cache for "+keys);
        Map loadedRows = connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).search(keys, missingKeys);

        //log.debug("Loaded rows: "+loadedRows.keySet());
        results.putAll(loadedRows);

        Collection keysToLoad = new TreeSet();
        keysToLoad.addAll(missingKeys);
        //keysToLoad.removeAll(results.keySet());
        //keysToLoad.removeAll(loadedRows.keySet());

        //log.debug("PKs to load: "+keysToLoad);
        if (!keysToLoad.isEmpty()) {
            Filter newFilter = connectorContext.getFilterTool().createFilter(keysToLoad);
            Map map = loadEntries(sourceDefinition, newFilter);
            results.putAll(map);

            Collection uniqueKeys = new TreeSet();

            for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                Row npk = connectorContext.getSchema().normalize(pk);
                AttributeValues values = (AttributeValues)map.get(pk);

                connectorContext.getSourceDataCache(connectionConfig, sourceDefinition).put(npk, values);

                Filter f = connectorContext.getFilterTool().createFilter(npk);
                Collection list = new TreeSet();
                list.add(npk);

                log.debug("Storing source filter cache "+f+": "+list);
                connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, list);

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
                Filter f = connectorContext.getFilterTool().createFilter(uniqueKeys);
                log.debug("Storing source filter cache "+f+": "+keys);
                connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(f, keys);
            }

            Collection list = new TreeSet();
            list.addAll(map.keySet());
            log.debug("Storing source filter cache "+newFilter+": "+list);
            connectorContext.getSourceFilterCache(connectionConfig, sourceDefinition).put(newFilter, list);
        }

        return results.values();
    }

    public SearchResults searchEntries(SourceDefinition sourceDefinition, Filter filter) throws Exception {

        SearchResults results = new SearchResults();

        String s = sourceDefinition.getParameter(SourceDefinition.SIZE_LIMIT);
        int sizeLimit = s == null ? SourceDefinition.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
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

            Row npk = connectorContext.getSchema().normalize(pk);
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

        Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());

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

        Connection connection = connectorContext.getConnection(sourceDefinition.getConnectionName());
        SearchResults sr;
        try {
            sr = connection.load(sourceDefinition, filter, sizeLimit);
        } catch (Exception e) {
            e.printStackTrace();
            return results;
        }

        //log.debug("Load results:");

        for (Iterator i=sr.iterator(); i.hasNext();) {
            AttributeValues av = (AttributeValues)i.next();

            Row pk = new Row();

            Collection fields = sourceDefinition.getFieldDefinitions();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                if (!fieldDefinition.isPrimaryKey()) continue;

                String name = fieldDefinition.getName();
                Collection values = av.get(name);
                if (values == null) {
                    pk = null;
                    break;
                }

                Object value = values.iterator().next();
                pk.set(name, value);
            }

            if (pk == null) continue;

            Row npk = connectorContext.getSchema().normalize(pk);
            //log.debug(" - PK: "+npk);

            results.put(npk, av);
        }

        return results;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public void setConnectorContext(ConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
    }
}
