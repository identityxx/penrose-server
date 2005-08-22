/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.sync;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SyncService {

    Logger log = LoggerFactory.getLogger(getClass());

    public Penrose penrose;

    public SyncService(Penrose penrose) throws Exception {
        this.penrose = penrose;
    }

    public int add(
            PenroseConnection connection,
            EntryDefinition entryDefinition,
            AttributeValues values)
            throws Exception {

        Date date = new Date();

        Graph graph = penrose.getGraph(entryDefinition);
        Source primarySource = penrose.getPrimarySource(entryDefinition);

        AddGraphVisitor visitor = new AddGraphVisitor(penrose, this, primarySource, entryDefinition, values, date);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();
/*
        Collection sources = entryDefinition.getSources();

        for (Iterator i2 = sources.iterator(); i2.hasNext(); ) {
            Source source = (Source)i2.next();

            int rc = add(source, entryDefinition, values, date);
            if (rc != LDAPException.SUCCESS) return rc;
        }

        engine.getEntryCache().put(entryDefinition, values, date);
*/
        penrose.getCache().getFilterCache().invalidate();

        return LDAPException.SUCCESS;
    }

    public int bind(Source source, EntryDefinition entry, AttributeValues attributes, String password, Date date) throws Exception {
        log.debug("----------------------------------------------------------------");
        log.debug("Binding as entry in "+source.getName());
        log.debug("Values: "+attributes);

        MRSWLock lock = penrose.getEngine().getLock(source);
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        try {

	        Map rows = penrose.getTransformEngine().transform(source, attributes);

	        log.debug("Entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Map pk = (Map)i.next();
	            AttributeValues row = (AttributeValues)rows.get(pk);

                Connection connection = penrose.getConnection(source.getConnectionName());
	            int rc = connection.bind(source, row, password);

	            if (rc != LDAPException.SUCCESS) return rc;
	        }

        } finally {
        	lock.releaseReadLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int add(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+source.getName());
        log.debug("Values: "+values);

        MRSWLock lock = penrose.getEngine().getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

	        Map rows = penrose.getTransformEngine().transform(source, values);

	        log.debug("New entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues fieldValues = (AttributeValues)rows.get(pk);

	            // Add row to the source table in the source database/directory
                Connection connection = penrose.getConnection(source.getConnectionName());
	            int rc = connection.add(source, fieldValues);
	            if (rc != LDAPException.SUCCESS) return rc;

	            // Add row to the source table in the cache
	            //getEngine().getSourceCache().put(source, pk, fieldValues);
	        }

        } finally {
        	lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int delete(EntryDefinition entryDefinition, AttributeValues values) throws Exception {

         Date date = new Date();

         Graph graph = penrose.getGraph(entryDefinition);
         Source primarySource = penrose.getPrimarySource(entryDefinition);

         DeleteGraphVisitor visitor = new DeleteGraphVisitor(penrose, this, primarySource, entryDefinition, values, date);
         graph.traverse(visitor, primarySource);

         if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();
/*
        Collection sources = entryDefinition.getSources();

        // delete from each source starting from the last one
        Source list[] = (Source[])sources.toArray(new Source[sources.size()]);

        for (int index=list.length-1; index>=0; index--) {
            Source source = list[index];

            int rc = delete(source, entryDefinition, values, date);

            if (rc == LDAPException.NO_SUCH_OBJECT) continue; // ignore
            if (rc != LDAPException.SUCCESS) return rc;
        }
*/
         penrose.getCache().getFilterCache().invalidate();

         Entry entry = new Entry(entryDefinition, values);
         penrose.getCache().getEntryCache().remove(entry);

         return LDAPException.SUCCESS;
     }

    public int delete(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

        log.info("-------------------------------------------------");
        log.debug("Deleting entry in "+source.getName());

        MRSWLock lock = penrose.getEngine().getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

	        log.debug("Values: "+values);

	        Map rows = penrose.getTransformEngine().transform(source, values);

	        log.debug("Entries: "+rows);

	        log.debug("Rows to be deleted from "+source.getName()+": "+rows.size()+" rows");

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);
                Connection connection = penrose.getConnection(source.getConnectionName());
	            int rc = connection.delete(source, attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            penrose.getCache().getSourceCache().remove(source, pk);
	        }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(Source source, EntryDefinition entry, AttributeValues oldValues, AttributeValues newValues, Date date) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Updating entry in " + source.getName());
        //log.debug("Old values: " + oldValues);
        //log.debug("New values: " + newValues);

        MRSWLock lock = penrose.getEngine().getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            Map oldEntries = penrose.getTransformEngine().transform(source, oldValues);
            Map newEntries = penrose.getTransformEngine().transform(source, newValues);

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
                Connection connection = penrose.getConnection(source.getConnectionName());
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
                Connection connection = penrose.getConnection(source.getConnectionName());
                int rc = connection.delete(source, oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                penrose.getCache().getSourceCache().remove(source, pk);
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
                Connection connection = penrose.getConnection(source.getConnectionName());
                int rc = connection.modify(source, oldEntry, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                penrose.getCache().getSourceCache().remove(source, pk);
                //engine.getSourceCache().insert(source, newEntry);
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldValues = entry.getAttributeValues();

        Date date = new Date();

        Graph graph = penrose.getGraph(entryDefinition);
        Source primarySource = penrose.getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(penrose, this, primarySource, entry, newValues, date);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();
/*
        Collection sources = entryDefinition.getSources();

        for (Iterator iterator = sources.iterator(); iterator.hasNext();) {
            Source source = (Source) iterator.next();

            int result = modify(source, entryDefinition, oldValues, newValues, date);

            if (result != LDAPException.SUCCESS) return result;
        }
*/
        penrose.getCache().getFilterCache().invalidate();
        penrose.getCache().getEntryCache().remove(entry);

        return LDAPException.SUCCESS;
    }

    public Collection search(Entry parent, EntryDefinition entryDefinition, Filter filter) throws Exception {
        Set keys = new HashSet();

        Source primarySource = penrose.getPrimarySource(entryDefinition);
        String primarySourceName = primarySource.getName();

        if (parent != null && parent.isDynamic()) {

            AttributeValues values = parent.getAttributeValues();
            Collection rows = penrose.getTransformEngine().convert(values);

            Collection newRows = new HashSet();
            for (Iterator i=rows.iterator(); i.hasNext(); ) {
                Row row = (Row)i.next();

                Interpreter interpreter = penrose.newInterpreter();
                interpreter.set(row);

                Row newRow = new Row();

                for (Iterator j=parent.getSources().iterator(); j.hasNext(); ) {
                    Source s = (Source)j.next();

                    for (Iterator k=s.getFields().iterator(); k.hasNext(); ) {
                        Field f = (Field)k.next();
                        String expression = f.getExpression();
                        Object v = interpreter.eval(expression);
                        if (v == null) continue;

                        //log.debug("Setting parent's value "+s.getName()+"."+f.getName()+": "+v);
                        newRow.set(f.getName(), v);
                    }
                }

                newRows.add(newRow);
            }

            String startingSourceName = getStartingSourceName(entryDefinition);
            Source startingSource = entryDefinition.getEffectiveSource(startingSourceName);

            Graph graph = penrose.getGraph(entryDefinition);

            SearchGraphVisitor visitor = new SearchGraphVisitor(penrose, entryDefinition, newRows, primarySource);
            graph.traverse(visitor, startingSource);
            keys.addAll(visitor.getKeys());

        } else {

            log.debug("Primary source: "+primarySourceName);

            Filter f = penrose.getCache().getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);

            log.debug("Searching source "+primarySourceName+" for "+f);
            Connection connection = penrose.getConnection(primarySource.getConnectionName());
            SearchResults results = connection.search(primarySource, f, 100);

            log.debug("Storing in source cache:");
            Map map = new HashMap();
            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Row pk = new Row();
                Collection fields = primarySource.getPrimaryKeyFields();
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
                //log.debug(" - "+pk+": "+values);
                penrose.getCache().getSourceCache().put(primarySource, pk, values);
            }

        }

        return keys;
    }

    public String getStartingSourceName(EntryDefinition entryDefinition) {

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsource = lhs.substring(0, li);
            Source ls = entryDefinition.getSource(lsource);
            if (ls == null) return lsource;

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsource = rhs.substring(0, ri);
            Source rs = entryDefinition.getSource(rsource);
            if (rs == null) return rsource;

        }

        Source source = (Source)entryDefinition.getSources().iterator().next();
        return source.getName();
    }

    public Collection load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection pks)
            throws Exception {

        Graph graph = penrose.getGraph(entryDefinition);
        Source primarySource = penrose.getPrimarySource(entryDefinition);

        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(penrose, this, entryDefinition, pks);
        graph.traverse(loaderVisitor, primarySource);

        Collection results = new ArrayList();

        Map attributeValues = loaderVisitor.getAttributeValues();
        for (Iterator i=attributeValues.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = (AttributeValues)attributeValues.get(pk);

            Collection c = penrose.getTransformEngine().convert(values);
            results.addAll(c);
        }
/*

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            JoinGraphVisitor joinerVisitor = new JoinGraphVisitor(entryDefinition, primarySource, sourceCache, pk);
            graph.traverse(joinerVisitor, primarySource);

            AttributeValues values = joinerVisitor.getAttributeValues();
            Collection c = getEngineContext().getTransformEngine().convert(values);

            results.addAll(c);
        }
*/
        log.debug("Rows:");

        for (Iterator j = results.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();
            log.debug(" - "+row);
        }

        log.debug("Loaded " + results.size() + " rows.");

        return results;
    }

    public Map load(
            Source source,
            Collection pks)
            throws Exception {

        log.info("Loading source "+source.getName()+" "+source.getSourceName()+" with pks "+pks);

        //CacheEvent beforeEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.BEFORE_LOAD_ENTRIES);
        //postCacheEvent(sourceConfig, beforeEvent);

        SourceCache sourceCache = penrose.getCache().getSourceCache();
        Engine engine = penrose.getEngine();

        Collection loadedPks = sourceCache.getPks(source, pks);
        log.debug("Loaded pks: "+loadedPks);

        Collection pksToLoad = new HashSet();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            boolean found = false;
            for (Iterator j=loadedPks.iterator(); !found && j.hasNext(); ) {
                Row lpk = (Row)j.next();
                if (penrose.getSchema().match(lpk, pk)) found = true;
            }

            if (!found) pksToLoad.add(pk);
        }
        log.debug("Pks to load: "+pksToLoad);

        Map results = new HashMap();

        for (Iterator i=loadedPks.iterator();  i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = sourceCache.get(source, pk);
            results.put(pk, values);
        }

        if (!pksToLoad.isEmpty()) {
            Filter filter = penrose.getCache().getCacheContext().getFilterTool().createFilter(pksToLoad);
            Connection connection = penrose.getConnection(source.getConnectionName());
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

                sourceCache.put(source, pk, values);
            }
        }

        //CacheEvent afterEvent = new CacheEvent(getCacheContext(), sourceConfig, CacheEvent.AFTER_LOAD_ENTRIES);
        //postCacheEvent(sourceConfig, afterEvent);

        return results;
    }

}
