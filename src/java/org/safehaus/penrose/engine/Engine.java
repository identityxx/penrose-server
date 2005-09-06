/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.*;
import org.safehaus.penrose.config.Config;
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

    private Map graphs = new HashMap();
    private Map primarySources = new HashMap();

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

    public void analyze(Config config) throws Exception {

        for (Iterator i=config.getRootEntryDefinitions().iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            analyze(entryDefinition);
        }
    }

    public void analyze(EntryDefinition entryDefinition) throws Exception {

        log.debug("Entry "+entryDefinition.getDn()+":");

        Source source = computePrimarySource(entryDefinition);
        if (source != null) {
            primarySources.put(entryDefinition, source);
            log.debug(" - primary source: "+source);
        }

        Graph graph = computeGraph(entryDefinition);
        if (graph != null) {
            graphs.put(entryDefinition, graph);
            log.debug(" - graph: "+graph);
        }

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Collection children = config.getChildren(entryDefinition);
        if (children != null) {
            for (Iterator i=children.iterator(); i.hasNext(); ) {
                EntryDefinition childDefinition = (EntryDefinition)i.next();
                analyze(childDefinition);
            }
        }
	}

    public Source getPrimarySource(EntryDefinition entryDefinition) {
        return (Source)primarySources.get(entryDefinition);
    }

    Source computePrimarySource(EntryDefinition entryDefinition) throws Exception {

        Collection rdnAttributes = entryDefinition.getRdnAttributes();

        // TODO need to handle multiple rdn attributes
        AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();
        String exp = rdnAttribute.getExpression().getScript();

        Interpreter interpreter = engineContext.newInterpreter();
        Collection variables = interpreter.parseVariables(exp);
        if (variables.size() == 0) return null;

        String primarySourceName = (String)variables.iterator().next();

        for (Iterator i = entryDefinition.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();
            if (source.getName().equals(primarySourceName)) return source;
        }

        return null;
    }

    public Map getGraphs() {
        return graphs;
    }

    public void setGraphs(Map graphs) {
        this.graphs = graphs;
    }

    public Map getPrimarySources() {
        return primarySources;
    }

    public void setPrimarySources(Map primarySources) {
        this.primarySources = primarySources;
    }

    public Graph getGraph(EntryDefinition entryDefinition) throws Exception {

        return (Graph)graphs.get(entryDefinition);
    }

    Graph computeGraph(EntryDefinition entryDefinition) throws Exception {

        Graph graph = new Graph();

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Collection sources = config.getEffectiveSources(entryDefinition);
        if (sources.size() == 0) return null;

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            graph.addNode(source);
        }

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            // System.out.println("Checking ["+relationship.getExpression()+"]");

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, li);

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, ri);

            Source lsource = config.getEffectiveSource(entryDefinition, lsourceName);
            Source rsource = config.getEffectiveSource(entryDefinition, rsourceName);
            graph.addEdge(lsource, rsource, relationship);
        }

        // System.out.println("Graph: "+graph);

        return graph;
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
            Entry parent,
            EntryDefinition entryDefinition,
            AttributeValues values)
            throws Exception {

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        AddGraphVisitor visitor = new AddGraphVisitor(getEngineContext(), primarySource, entryDefinition, values);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        String key = entryDefinition.getRdn()+","+parent.getDn();
        engineContext.getCache().getEntryFilterCache().remove(key);

        return LDAPException.SUCCESS;
    }

    public int delete(Entry entry) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues values = entry.getAttributeValues();

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(getEngineContext(), primarySource, entryDefinition, values);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        String key = entryDefinition.getRdn()+","+entry.getParent().getDn();

        getEngineContext().getCache().getEntryDataCache().remove(key, entry.getRdn());

        engineContext.getCache().getEntryFilterCache().remove(key);

        return LDAPException.SUCCESS;
    }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldValues = entry.getAttributeValues();

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(getEngineContext(), primarySource, entry, newValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        String key = entryDefinition.getRdn()+","+entry.getParent().getDn();

        getEngineContext().getCache().getEntryDataCache().remove(key, entry.getRdn());
        engineContext.getCache().getEntryFilterCache().remove(key);

        return LDAPException.SUCCESS;
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param rdns
     * @throws Exception
     */
    public void load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdns,
            SearchResults results
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading entry "+entryDefinition.getDn()+" with rdns "+rdns);

        MRSWLock lock = getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            Collection rdnsToLoad = new TreeSet();

            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();

                String dn = entryDefinition.getRdn()+","+parent.getDn();

                Entry entry = getEngineContext().getCache().getEntryDataCache().get(dn, rdn);
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

                Collection filters = rdnToFilter(entryDefinition, rdnsToLoad);
                Map avs = loadEntries(parent, entryDefinition, filters);
                Collection entries = getEngineContext().getTransformEngine().merge(parent, entryDefinition, avs);

                String dn = entryDefinition.getRdn()+","+parent.getDn();

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    entry.setParent(parent);
                    results.add(entry);
                    getEngineContext().getCache().getEntryDataCache().put(dn, entry.getRdn(), entry);
                }
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }
    }

    public Collection rdnToFilter(EntryDefinition entryDefinition, Collection rdns) throws Exception {

        Collection filters = new TreeSet();

        Source primarySource = getPrimarySource(entryDefinition);
        Collection fields = primarySource.getFields();

        //log.debug("Creating filters:");
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();
            //log.debug(" - "+rdn);

            Interpreter interpreter = engineContext.newInterpreter();
            interpreter.set(rdn);

            Row filter = new Row();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                Expression exp = field.getExpression();
                if (exp == null) continue;

                String script = exp.getScript();
                //log.debug("   - "+field.getName()+": "+script);
                Object value = interpreter.eval(script);
                if (value == null) continue;

                filter.set(field.getName(), value);
            }

            filters.add(filter);
        }

        return filters;
    }

    public Map loadEntries(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection filters)
            throws Exception {

        log.debug("Loading: "+filters);

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(getEngineContext(), entryDefinition, filters);
        graph.traverse(loaderVisitor, primarySource);

        Map attributeValues = loaderVisitor.getAttributeValues();
        return attributeValues;
/*
        //log.debug("Rows:");
        Collection results = new ArrayList();
        for (Iterator i=attributeValues.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            //log.debug(" - "+pk);

            AttributeValues values = (AttributeValues)attributeValues.get(pk);

            Collection c = getEngineContext().getTransformEngine().convert(values);
            results.addAll(c);
        }

        log.debug("Loaded " + results.size() + " rows.");

        return results;
*/
    }


    public Collection search(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching for entry "+entryDefinition.getDn()+" with filter "+filter);

        String dn = entryDefinition.getRdn()+","+parent.getDn();
        log.debug("Checking entry filter cache for ["+dn+"]");

        Collection rdns = engineContext.getCache().getEntryFilterCache().get(dn, filter);

        if (rdns != null) {
            log.debug("Entry filter cache found: "+filter);
            return rdns;
        }

        log.debug("Entry filter cache not found.");
        log.debug("Searching entry "+dn+" for "+filter);

        Source primarySource = getPrimarySource(entryDefinition);
        String primarySourceName = primarySource.getName();
        Collection keys = searchEntries(parent, entryDefinition, filter);

        //log.debug("Search results:");
        rdns = new TreeSet();

        for (Iterator j=keys.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();
            //log.debug(" - "+row);

            Interpreter interpreter = engineContext.newInterpreter();
            for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                Object value = row.get(name);
                interpreter.set(primarySourceName+"."+name, value);
            }

            Collection rdnAttributes = entryDefinition.getRdnAttributes();

            Row rdn = new Row();
            boolean valid = true;

            for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                AttributeDefinition attr = (AttributeDefinition)k.next();
                String name = attr.getName();
                String expression = attr.getExpression().getScript();
                Object value = interpreter.eval(expression);

                if (value == null) {
                    valid = false;
                    break;
                }

                rdn.set(name, value);
            }

            if (!valid) continue;

            Row nrdn = engineContext.getSchema().normalize(rdn);
            //log.debug(" - RDN: "+nrdn);

            rdns.add(nrdn);
        }

        engineContext.getCache().getEntryFilterCache().put(dn, filter, rdns);

        filter = engineContext.getCache().getCacheContext().getFilterTool().createFilter(rdns);
        engineContext.getCache().getEntryFilterCache().put(dn, filter, rdns);

        return rdns;
    }

    public Collection searchEntries(Entry parent, EntryDefinition entryDefinition, Filter filter) throws Exception {
        Source primarySource = getPrimarySource(entryDefinition);

        Collection newRows = null;

        if (parent.getSources().size() > 0) {

            AttributeValues values = parent.getAttributeValues();
            Collection rows = getEngineContext().getTransformEngine().convert(values);

            newRows = new HashSet();
            log.debug("Parent's values:");

            for (Iterator i=rows.iterator(); i.hasNext(); ) {
                Row row = (Row)i.next();

                Interpreter interpreter = getEngineContext().newInterpreter();
                interpreter.set(row);

                Row newRow = new Row();

                for (Iterator j=parent.getSources().iterator(); j.hasNext(); ) {
                    Source s = (Source)j.next();

                    for (Iterator k=s.getFields().iterator(); k.hasNext(); ) {
                        Field f = (Field)k.next();

                        Expression expression = f.getExpression();
                        if (expression == null) continue;

                        String script = expression.getScript();
                        Object v = interpreter.eval(script);
                        if (v == null) continue;

                        newRow.set(f.getName(), v);
                    }
                }

                log.debug(" - "+newRow);
                newRows.add(newRow);
            }
        }

        Config config = engineContext.getConfig(entryDefinition.getDn());

        String startingSourceName = getStartingSourceName(entryDefinition);
        Source startingSource = config.getEffectiveSource(entryDefinition, startingSourceName);

        Graph graph = getGraph(entryDefinition);

        Collection keys = new TreeSet();

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

