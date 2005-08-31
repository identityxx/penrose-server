/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.sync;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SyncService {

    public final static int WAIT_TIMEOUT = 10000; // wait timeout is 10 seconds

    Logger log = LoggerFactory.getLogger(getClass());

    public SyncContext syncContext;

    private Map locks = new HashMap();
    private Queue queue = new Queue();

    public SyncService(SyncContext syncContext) throws Exception {
        this.syncContext = syncContext;
    }

    public synchronized MRSWLock getLock(Source source) {
		String name = source.getConnectionName() + "." + source.getSourceName();

		MRSWLock lock = (MRSWLock)locks.get(name);

		if (lock == null) lock = new MRSWLock(queue);
		locks.put(name, lock);

		return lock;
	}

    public int bind(Source source, EntryDefinition entry, AttributeValues attributes, String password) throws Exception {
        log.debug("----------------------------------------------------------------");
        log.debug("Binding as entry in "+source.getName());
        log.debug("Values: "+attributes);

        MRSWLock lock = getLock(source);
        lock.getReadLock(WAIT_TIMEOUT);

        try {

	        Map rows = syncContext.getTransformEngine().transform(source, attributes);

	        log.debug("Entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Map pk = (Map)i.next();
	            AttributeValues row = (AttributeValues)rows.get(pk);

                Connection connection = syncContext.getConnection(source.getConnectionName());
	            int rc = connection.bind(source, row, password);

	            if (rc != LDAPException.SUCCESS) return rc;
	        }

        } finally {
        	lock.releaseReadLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int add(Source source, EntryDefinition entry, AttributeValues values) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+source.getName());
        log.debug("Values: "+values);

        MRSWLock lock = getLock(source);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {

	        Map rows = syncContext.getTransformEngine().transform(source, values);

	        log.debug("New entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues fieldValues = (AttributeValues)rows.get(pk);

	            // Add row to the source table in the source database/directory
                Connection connection = syncContext.getConnection(source.getConnectionName());
	            int rc = connection.add(source, fieldValues);
	            if (rc != LDAPException.SUCCESS) return rc;
	        }

            String key = source.getConnectionConfig().getConnectionName()+"."+source.getSourceName();
            syncContext.getCache().getSourceFilterCache().remove(key);

        } finally {
        	lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(Source source, EntryDefinition entry, AttributeValues values) throws Exception {

        log.info("-------------------------------------------------");
        log.debug("Deleting entry in "+source.getName());

        MRSWLock lock = getLock(source);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {

	        log.debug("Values: "+values);

	        Map rows = syncContext.getTransformEngine().transform(source, values);

	        log.debug("Entries: "+rows);

	        log.debug("Rows to be deleted from "+source.getName()+": "+rows.size()+" rows");

            String key = source.getConnectionConfig().getConnectionName()+"."+source.getSourceName();

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);
                Connection connection = syncContext.getConnection(source.getConnectionName());
	            int rc = connection.delete(source, attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            syncContext.getCache().getSourceDataCache().remove(key, pk);
	        }

            syncContext.getCache().getSourceFilterCache().remove(key);

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(Source source, EntryDefinition entry, AttributeValues oldValues, AttributeValues newValues) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Updating entry in " + source.getName());
        //log.debug("Old values: " + oldValues);
        //log.debug("New values: " + newValues);

        MRSWLock lock = getLock(source);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {

            Map oldEntries = syncContext.getTransformEngine().transform(source, oldValues);
            Map newEntries = syncContext.getTransformEngine().transform(source, newValues);

            //log.debug("Old entries: " + oldEntries);
            //log.debug("New entries: " + newEntries);

            Collection oldPKs = oldEntries.keySet();
            Collection newPKs = newEntries.keySet();

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

            String key = source.getConnectionConfig().getConnectionName()+"."+source.getSourceName();

            // Add rows
            for (Iterator i = addRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues) newEntries.get(pk);
                log.debug("ADDING ROW: " + newEntry);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(newEntry);

                // Add row to source table in the source database/directory
                Connection connection = syncContext.getConnection(source.getConnectionName());
                int rc = connection.add(source, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;
            }

            // Remove rows
            for (Iterator i = removeRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues) oldEntries.get(pk);
                log.debug("DELETE ROW: " + oldEntry);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(oldEntry);

                // Delete row from source table in the source database/directory
                Connection connection = syncContext.getConnection(source.getConnectionName());
                int rc = connection.delete(source, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                syncContext.getCache().getSourceDataCache().remove(key, pk);
            }

            // Replace rows
            for (Iterator i = replaceRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues) oldEntries.get(pk);
                AttributeValues newEntry = (AttributeValues) newEntries.get(pk);
                log.debug("REPLACE ROW: " + newEntry.toString());

                //AttributeValues oldAttributes = engineContext.getTransformEngine().convert(oldEntry);
                //AttributeValues newAttributes = engineContext.getTransformEngine().convert(newEntry);

                // Modify row from source table in the source database/directory
                Connection connection = syncContext.getConnection(source.getConnectionName());
                int rc = connection.modify(source, oldEntry, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                syncContext.getCache().getSourceDataCache().remove(key, pk);
            }

            syncContext.getCache().getSourceFilterCache().remove(key);

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public Map search(
            Source source,
            Collection filters)
            throws Exception {

        Collection normalizedFilters = null;
        if (filters != null) {
            normalizedFilters = new TreeSet();
            for (Iterator i=filters.iterator(); i.hasNext(); ) {
                Row filter = (Row)i.next();
                Row f = new Row();
                for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    if (source.getField(name) == null) continue;
                    f.set(name, filter.get(name));
                }
                Row normalizedFilter = syncContext.getSchema().normalize(f);
                normalizedFilters.add(normalizedFilter);
            }
        }

        Filter filter = null;
        if (filters != null) {
            filter = syncContext.getCache().getCacheContext().getFilterTool().createFilter(normalizedFilters);
        }

        log.debug("Searching source "+source.getName()+" "+source.getSourceName()+" with filter "+filter);

        String key = source.getConnectionConfig().getConnectionName()+"."+source.getSourceName();
        log.debug("Checking source filter cache for ["+key+"]");

        Map results = new TreeMap();

        Collection pks = syncContext.getCache().getSourceFilterCache().get(key, filter);

        if (pks == null) {

            String method = source.getParameter("loadingMethod");
            log.debug("Loading method: "+method);
            
            if ("searchAndLoad".equals(method)) {
                log.debug("Searching pks for: "+filter);
                pks = searchEntries(source, filter);

            } else {
                log.debug("Loading entries for: "+filter);
                Map map = loadEntries(source, filter);
                pks = map.keySet();
                results.putAll(map);

                for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
                    Row pk = (Row)i.next();
                    AttributeValues values = (AttributeValues)map.get(pk);
                    syncContext.getCache().getSourceDataCache().put(key, pk, values);
                }
            }

            syncContext.getCache().getSourceFilterCache().put(key, filter, pks);

            Filter newFilter = syncContext.getCache().getCacheContext().getFilterTool().createFilter(pks);
            syncContext.getCache().getSourceFilterCache().put(key, newFilter, pks);
        }

        log.debug("Checking source cache for pks "+pks);
        Map loadedRows = syncContext.getCache().getSourceDataCache().get(key, pks);
        log.debug("Loaded rows: "+loadedRows.keySet());
        results.putAll(loadedRows);

        Collection pksToLoad = new HashSet();
        pksToLoad.addAll(pks);
        pksToLoad.removeAll(results.keySet());
        pksToLoad.removeAll(loadedRows.keySet());

        if (!pksToLoad.isEmpty()) {
            log.debug("Loading pks: "+pksToLoad);
            Filter newFilter = syncContext.getCache().getCacheContext().getFilterTool().createFilter(pksToLoad);
            Map map = loadEntries(source, newFilter);
            results.putAll(map);

            for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                AttributeValues values = (AttributeValues)map.get(pk);
                syncContext.getCache().getSourceDataCache().put(key, pk, values);
            }

            syncContext.getCache().getSourceFilterCache().put(key, newFilter, map.keySet());
        }

        return results;
    }

    public Collection searchEntries(Source source, Filter filter) throws Exception {

        Collection results = new TreeSet();

        Connection connection = syncContext.getConnection(source.getConnectionName());
        SearchResults sr = connection.search(source, filter, 100);

        log.debug("Search results:");

        for (Iterator i=sr.iterator(); i.hasNext();) {
            Row pk = (Row)i.next();

            Row npk = syncContext.getSchema().normalize(pk);
            log.debug(" - PK: "+npk);

            results.add(npk);
        }

        return results;
    }

    public Map loadEntries(Source source, Filter filter) throws Exception {

        Map results = new TreeMap();

        Connection connection = syncContext.getConnection(source.getConnectionName());
        SearchResults sr = connection.load(source, filter, 100);

        log.debug("Load results:");

        for (Iterator i=sr.iterator(); i.hasNext();) {
            AttributeValues av = (AttributeValues)i.next();

            Row pk = new Row();

            Collection fields = source.getPrimaryKeyFields();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                String name = field.getName();
                Object value = av.get(name).iterator().next();

                pk.set(name, value);
            }

            Row npk = syncContext.getSchema().normalize(pk);
            log.debug(" - PK: "+npk);

            results.put(npk, av);
        }

        return results;
    }

}
