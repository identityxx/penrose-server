/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Engine {

    Logger log = LoggerFactory.getLogger(getClass());

    private EngineContext engineContext;

    private Map locks = new HashMap();
    private Queue queue = new Queue();

    private ThreadPool threadPool = null;

    private boolean stopping = false;

    /**
     * Initialize the engine with a Penrose instance
     *
     * @param engineContext
     * @throws Exception
     */
    public void init(EngineConfig engineConfig, EngineContext engineContext) throws Exception {
        this.engineContext = engineContext;

        log.debug("-------------------------------------------------");
        log.debug("Initializing Engine");

        // Now size is now hardcoded to 20
        // TODO modify size to read from configuration if needed
        int size = 20;
        threadPool = new ThreadPool(size);

        execute(new RefreshThread(this));
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

    public boolean isStopping() {
        return stopping;
    }

	public synchronized MRSWLock getLock(String dn) {

		MRSWLock lock = (MRSWLock)locks.get(dn);

		if (lock == null) lock = new MRSWLock(queue);
		locks.put(dn, lock);

		return lock;
	}

    public Collection merge(Entry parent, EntryDefinition entryDefinition, Collection rows) throws Exception {

        Collection results = new ArrayList();

        //log.debug("Merging:");
        // merge rows into attribute values
        Map entries = new LinkedHashMap();
        for (Iterator i = rows.iterator(); i.hasNext();) {
            Row row = (Row)i.next();
            //log.debug(" - "+row);

            Map rdn = new HashMap();
            Row values = new Row();

            boolean validPK = getEngineContext().getTransformEngine().translate(entryDefinition, row, rdn, values);
            if (!validPK) continue;

            //log.debug(" - "+rdn+": "+values);

            AttributeValues attributeValues = (AttributeValues)entries.get(rdn);
            if (attributeValues == null) {
                attributeValues = new AttributeValues();
                entries.put(rdn, attributeValues);
            }
            attributeValues.add(values);
        }

        log.debug("Merged into " + entries.size() + " entries.");

        int counter = 1;
        for (Iterator i=entries.values().iterator(); i.hasNext(); counter++) {
            AttributeValues values = (AttributeValues)i.next();

            Entry entry = new Entry(entryDefinition, values);
            entry.setParent(parent);
            results.add(entry);

            log.debug("Entry #"+counter+":\n"+entry+"\n");
        }

        return results;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
    }

    public int add(
            EntryDefinition entryDefinition,
            AttributeValues values)
            throws Exception {

        Date date = new Date();

        Graph graph = getEngineContext().getGraph(entryDefinition);
        Source primarySource = getEngineContext().getPrimarySource(entryDefinition);

        AddGraphVisitor visitor = new AddGraphVisitor(getEngineContext(), getEngineContext().getSyncService(), primarySource, entryDefinition, values, date);
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
        return LDAPException.SUCCESS;
    }

    public int delete(EntryDefinition entryDefinition, AttributeValues values) throws Exception {

         Date date = new Date();

         Graph graph = getEngineContext().getGraph(entryDefinition);
         Source primarySource = getEngineContext().getPrimarySource(entryDefinition);

         DeleteGraphVisitor visitor = new DeleteGraphVisitor(getEngineContext(), getEngineContext().getSyncService(), primarySource, entryDefinition, values, date);
         graph.traverse(visitor, primarySource);

         if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

         Entry entry = new Entry(entryDefinition, values);
         getEngineContext().getCache().getEntryCache().remove(entry);

         return LDAPException.SUCCESS;
     }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldValues = entry.getAttributeValues();

        Date date = new Date();

        Graph graph = getEngineContext().getGraph(entryDefinition);
        Source primarySource = getEngineContext().getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(getEngineContext(), getEngineContext().getSyncService(), primarySource, entry, newValues, date);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        getEngineContext().getCache().getEntryCache().remove(entry);

        return LDAPException.SUCCESS;
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param rdns
     * @param calendar
     * @throws Exception
     */
    public SearchResults load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdns,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading entry "+entryDefinition.getDn()+" with rdns "+rdns);

        MRSWLock lock = getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {
            String s = getEngineContext().getCache().getParameter(CacheConfig.CACHE_EXPIRATION);
            int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
            log.debug("Expiration: "+cacheExpiration);
            if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

            Calendar c = (Calendar) calendar.clone();
            c.add(Calendar.MINUTE, -cacheExpiration);

            Collection rdnsToLoad = new TreeSet();

            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();

                String dn = rdn.toString()+","+parent.getDn();

                Entry entry = getEngineContext().getCache().getEntryCache().get(dn);
                if (entry == null) {
                    rdnsToLoad.add(rdn);
                } else {
                    entry.setParent(parent);
                    entry.setEntryDefinition(entryDefinition);
                    results.add(entry);
                }
            }

            log.debug("Rdns to load: "+rdnsToLoad);

            if (!rdnsToLoad.isEmpty()) {

                Collection pks = rdnToPk(entryDefinition, rdnsToLoad);
                Collection rows = load(parent, entryDefinition, pks);
                Collection entries = merge(parent, entryDefinition, rows);

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    entry.setParent(parent);
                    results.add(entry);
                    getEngineContext().getCache().getEntryCache().put(entry);
                }
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }

        return results;
    }

    public Collection rdnToPk(EntryDefinition entryDefinition, Collection rdns) throws Exception {

        Collection attributes = entryDefinition.getRdnAttributes();

        // TODO need to handle composite rdn
        AttributeDefinition attribute = (AttributeDefinition)attributes.iterator().next();
        String expression = attribute.getExpression();

        // TODO need to handle complex expression
        int pos = expression.indexOf(".");
        String sourceName = expression.substring(0, pos);
        String fieldName = expression.substring(pos+1);

        Collection pks = new TreeSet();

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            // TODO need to handle composite rdn
            String attributeName = (String)rdn.getNames().iterator().next();
            Object value = rdn.get(attributeName);

            Row pk = new Row();
            pk.set(fieldName, value);
            pks.add(pk);
        }

        return pks;
    }

    public Collection load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection pks)
            throws Exception {

        log.debug("Loading: "+pks);

        Graph graph = getEngineContext().getGraph(entryDefinition);
        Source primarySource = getEngineContext().getPrimarySource(entryDefinition);

        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(getEngineContext(), getEngineContext().getSyncService(), entryDefinition, pks);
        graph.traverse(loaderVisitor, primarySource);

        Collection results = new ArrayList();

        Map attributeValues = loaderVisitor.getAttributeValues();

        log.debug("Rows:");
        for (Iterator i=attributeValues.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            log.debug(" - "+pk);

            AttributeValues values = (AttributeValues)attributeValues.get(pk);

            Collection c = getEngineContext().getTransformEngine().convert(values);
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

    public Collection search(Entry parent, EntryDefinition entryDefinition, Filter filter) throws Exception {
        Source primarySource = getEngineContext().getPrimarySource(entryDefinition);

        if (parent == null || !parent.isDynamic()) {

            log.debug("Primary source: "+primarySource.getName());

            Filter f = engineContext.getCache().getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);
            return engineContext.getSyncService().search(primarySource, f);
        }

        AttributeValues values = parent.getAttributeValues();
        Collection rows = getEngineContext().getTransformEngine().convert(values);

        Collection newRows = new HashSet();
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            Interpreter interpreter = getEngineContext().newInterpreter();
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

        Graph graph = getEngineContext().getGraph(entryDefinition);

        Collection keys = new HashSet();

        SearchGraphVisitor visitor = new SearchGraphVisitor(getEngineContext(), entryDefinition, newRows, primarySource);
        graph.traverse(visitor, startingSource);
        keys.addAll(visitor.getKeys());

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

}

