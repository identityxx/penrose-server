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

    private SearchEngine searchEngine;
    private JoinEngine joinEngine;

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

        searchEngine = new SearchEngine(this);
        joinEngine = new JoinEngine(this);

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
            AttributeValues attributeValues)
            throws Exception {

        AttributeValues sourceValues = new AttributeValues();
        getFieldValues("parent", parent, sourceValues);

        Collection sources = entryDefinition.getSources();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            Row pk = engineContext.getTransformEngine().translate(source, attributeValues, output);
            if (pk == null) continue;

            log.debug(" - "+pk+": "+output);
            for (Iterator j=output.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = output.get(name);
                sourceValues.add(source.getName()+"."+name, values);
            }
        }

        log.debug("Adding entry "+entryDefinition.getRdn()+","+parent.getDn()+" with values: "+sourceValues);

        Config config = engineContext.getConfig(entryDefinition.getDn());

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);
        String startingSourceName = getStartingSourceName(entryDefinition);
        Source startingSource = config.getEffectiveSource(entryDefinition, startingSourceName);

        log.debug("Starting from source: "+startingSourceName);
        Collection relationships = graph.getEdgeObjects(startingSource);
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            log.debug("Relationship "+relationship);

            String lhs = relationship.getLhs();
            String rhs = relationship.getRhs();

            if (rhs.startsWith(startingSourceName+".")) {
                String exp = lhs;
                lhs = rhs;
                rhs = exp;
            }

            Collection lhsValues = sourceValues.get(lhs);
            log.debug(" - "+lhs+" -> "+rhs+": "+lhsValues);
            sourceValues.set(rhs, lhsValues);
        }

        AddGraphVisitor visitor = new AddGraphVisitor(engineContext, entryDefinition, sourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getCache().getEntryFilterCache(parent, entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int delete(Entry entry) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues sourceValues = entry.getSourceValues();

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(engineContext, entryDefinition, sourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getCache().getEntryDataCache(entry.getParent(), entryDefinition).remove(entry.getRdn());
        engineContext.getCache().getEntryFilterCache(entry.getParent(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(engineContext, primarySource, entry, newValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getCache().getEntryDataCache(entry.getParent(), entryDefinition).remove(entry.getRdn());
        engineContext.getCache().getEntryFilterCache(entry.getParent(), entryDefinition).invalidate();

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
        log.debug("Loading entry under "+parent.getDn()+" with rdns "+rdns);

        MRSWLock lock = getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            Collection rdnsToLoad = new TreeSet();

            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();

                Entry entry = engineContext.getCache().getEntryDataCache(parent, entryDefinition).get(rdn);
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

                Collection entries = joinEngine.load(parent, entryDefinition, rdnsToLoad);

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    entry.setParent(parent);
                    results.add(entry);
                    engineContext.getCache().getEntryDataCache(parent, entryDefinition).put(entry.getRdn(), entry);
                }
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }
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

        Collection rdns = engineContext.getCache().getEntryFilterCache(parent, entryDefinition).get(filter);

        if (rdns != null) {
            log.debug("Entry filter cache found: "+filter);
            return rdns;
        }

        log.debug("Entry filter cache not found.");
        log.debug("Searching entry "+dn+" for "+filter);

        rdns = searchEngine.search(parent, entryDefinition, filter);

        engineContext.getCache().getEntryFilterCache(parent, entryDefinition).put(filter, rdns);

        filter = engineContext.getCache().getCacheContext().getFilterTool().createFilter(rdns);
        engineContext.getCache().getEntryFilterCache(parent, entryDefinition).put(filter, rdns);

        return rdns;
    }

    public void getFieldValues(Entry entry, AttributeValues results) throws Exception {
        getFieldValues(null, entry, results);
    }

    public void getFieldValues(String prefix, Entry entry, AttributeValues results) throws Exception {
        if (entry.getParent() != null) {
            getFieldValues((prefix == null ? "" : prefix+".")+"parent", entry.getParent(), results);
        }

        log.debug(entry.getDn()+"'s source values:");

        Collection sources = entry.getSources();
        if (sources.size() == 0) return;

        AttributeValues sourceValues = entry.getSourceValues();
        if (sourceValues != null) {
            for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection value = sourceValues.get(name);
                log.debug(" - "+name+": "+value);
            }
            results.add(sourceValues);
        }

        log.debug(entry.getDn()+"'s attribute values:");
/*
        AttributeValues attributeValues = entry.getAttributeValues();

        for (Iterator i=values.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection value = values.get(name);
            log.debug(" - "+prefix+"."+name+": "+value);
            results.set(prefix+"."+name, value);
        }
*/
/*
        for (Iterator i=entry.getSources().iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            getEngineContext().getTransformEngine().translate(source, attributeValues, output);

            for (Iterator j=output.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection value = output.get(name);
                log.debug(" - "+source.getName()+"."+name+": "+value);
                results.set(source.getName()+"."+name, value);
            }
        }
*/
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

    public SearchEngine getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    public JoinEngine getJoinEngine() {
        return joinEngine;
    }

    public void setJoinEngine(JoinEngine joinEngine) {
        this.joinEngine = joinEngine;
    }
}

