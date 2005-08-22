/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.sync;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.cache.SourceCache;
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

    public int bind(Source source, EntryDefinition entry, AttributeValues attributes, String password, Date date) throws Exception {
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

    public int add(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

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

	            // Add row to the source table in the cache
	            //getEngine().getSourceCache().put(source, pk, fieldValues);
	        }

        } finally {
        	lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

        log.info("-------------------------------------------------");
        log.debug("Deleting entry in "+source.getName());

        MRSWLock lock = getLock(source);
        lock.getWriteLock(WAIT_TIMEOUT);

        try {

	        log.debug("Values: "+values);

	        Map rows = syncContext.getTransformEngine().transform(source, values);

	        log.debug("Entries: "+rows);

	        log.debug("Rows to be deleted from "+source.getName()+": "+rows.size()+" rows");

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);
                Connection connection = syncContext.getConnection(source.getConnectionName());
	            int rc = connection.delete(source, attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            syncContext.getCache().getSourceCache().remove(source, pk);
	        }

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(Source source, EntryDefinition entry, AttributeValues oldValues, AttributeValues newValues, Date date) throws Exception {

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

                // Add row to source table in the cache
                //engine.getSourceCache().insert(source, newEntry, date);
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
                syncContext.getCache().getSourceCache().remove(source, pk);
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
                syncContext.getCache().getSourceCache().remove(source, pk);
                //engine.getSourceCache().insert(source, newEntry);
            }

        } finally {
            lock.releaseWriteLock(WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public Map load(
            Source source,
            Collection pks)
            throws Exception {

        log.info("Loading source "+source.getName()+" "+source.getSourceName()+" with pks "+pks);

        //CacheEvent beforeEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.BEFORE_LOAD_ENTRIES);
        //postCacheEvent(sourceConfig, beforeEvent);

        Collection loadedPks = syncContext.getCache().getSourceCache().getPks(source, pks);
        log.debug("Loaded pks: "+loadedPks);

        Collection pksToLoad = new HashSet();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            boolean found = false;
            for (Iterator j=loadedPks.iterator(); !found && j.hasNext(); ) {
                Row lpk = (Row)j.next();
                if (syncContext.getSchema().match(lpk, pk)) found = true;
            }

            if (!found) pksToLoad.add(pk);
        }
        log.debug("Pks to load: "+pksToLoad);

        Map results = new HashMap();

        for (Iterator i=loadedPks.iterator();  i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = syncContext.getCache().getSourceCache().get(source, pk);
            results.put(pk, values);
        }

        if (!pksToLoad.isEmpty()) {
            Filter filter = syncContext.getCache().getCacheContext().getFilterTool().createFilter(pksToLoad);
            Connection connection = syncContext.getConnection(source.getConnectionName());
            SearchResults sr = connection.search(source, filter, 0);

            for (Iterator j = sr.iterator(); j.hasNext();) {
                Row row = (Row) j.next();

                Row pk = new Row();

                Collection fields = source.getPrimaryKeyFields();
                for (Iterator i=fields.iterator(); i.hasNext(); ) {
                    Field field = (Field)i.next();
                    String name = field.getName();
                    Object value = row.get(name);

                    pk.set(name, value);
                }

                AttributeValues values = (AttributeValues)results.get(pk);
                if (values == null) {
                    values = new AttributeValues();
                    results.put(pk, values);
                }

                values.add(row); // merging row
            }

            for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                AttributeValues values = (AttributeValues)results.get(pk);

                syncContext.getCache().getSourceCache().put(source, pk, values);
            }
        }

        //CacheEvent afterEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.AFTER_LOAD_ENTRIES);
        //postCacheEvent(sourceConfig, afterEvent);

        return results;
    }

    public Collection search(Source source, Filter filter) throws Exception {
        Set keys = new HashSet();

        log.debug("Searching source "+source.getName()+" for "+filter);
        Connection connection = syncContext.getConnection(source.getConnectionName());
        SearchResults results = connection.search(source, filter, 100);
        if (results.size() == 0) return keys;

        log.debug("Storing in source cache:");
        Map map = new HashMap();
        for (Iterator j=results.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();

            Row pk = new Row();
            Collection fields = source.getPrimaryKeyFields();
            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                Field field = (Field)i.next();
                Object value = row.get(field.getName());
                pk.set(field.getName(), value);
            }

            AttributeValues values = (AttributeValues)map.get(pk);
            if (values == null) {
                values = new AttributeValues();
                map.put(pk, values);
            }
            values.add(row);

            keys.add(row);
        }

        for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
            Row pk = (Row)j.next();
            AttributeValues values = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+values);
            syncContext.getCache().getSourceCache().put(source, pk, values);
        }

        return keys;
    }
}
