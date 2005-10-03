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
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

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

    private EngineFilterTool filterTool;
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

        filterTool = new EngineFilterTool(engineContext);
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
        Expression expression = rdnAttribute.getExpression();

        Interpreter interpreter = engineContext.newInterpreter();

        if (expression.getForeach() != null) {
            Collection variables = interpreter.parseVariables(expression.getForeach());

            for (Iterator i=variables.iterator(); i.hasNext(); ) {
                String sourceName = (String)i.next();
                Source source = entryDefinition.getSource(sourceName);
                if (source != null) return source;
            }
        }

        Collection variables = interpreter.parseVariables(expression.getScript());

        for (Iterator i=variables.iterator(); i.hasNext(); ) {
            String sourceName = (String)i.next();
            Source source = entryDefinition.getSource(sourceName);
            if (source != null) return source;
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
            log.debug("Checking ["+relationship.getExpression()+"]");

            Set edge = new HashSet();
            for (Iterator j = relationship.getOperands().iterator(); j.hasNext(); ) {
                String operand = j.next().toString();

                int index = operand.indexOf(".");
                if (index < 0) continue;

                String sourceName = operand.substring(0, index);
                Source source = config.getEffectiveSource(entryDefinition, sourceName);
                if (source == null) continue;

                edge.add(source);
            }

            graph.addEdge(edge, relationship);
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
        if (startingSourceName == null) return LDAPException.SUCCESS;

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

        engineContext.getEntryFilterCache(parent, entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int delete(Entry entry) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();

        AttributeValues sourceValues = new AttributeValues();
        getFieldValues(entry, sourceValues);

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        log.debug("Deleting entry "+entry.getDn()+" ["+sourceValues+"]");

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(engineContext, entryDefinition, sourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getEntryDataCache(entry.getParent(), entryDefinition).remove(entry.getRdn());
        engineContext.getEntryFilterCache(entry.getParent(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int modify(Entry entry, AttributeValues oldValues, AttributeValues newValues) throws Exception {

        Entry parent = entry.getParent();
        EntryDefinition entryDefinition = entry.getEntryDefinition();
        Collection sources = entryDefinition.getSources();

        AttributeValues oldSourceValues = new AttributeValues();
        getFieldValues(entry, oldSourceValues);

        AttributeValues newSourceValues = (AttributeValues)oldSourceValues.clone();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            engineContext.getTransformEngine().translate(source, newValues, output);

            log.debug(" - "+output);
            for (Iterator j=output.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = output.get(name);
                newSourceValues.set(source.getName()+"."+name, values);
            }
        }

        log.debug("Modifying entry "+entryDefinition.getRdn()+","+parent.getDn()+" ["+oldSourceValues+"] with: "+newSourceValues);

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(engineContext, entryDefinition, oldSourceValues, newSourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getEntryDataCache(entry.getParent(), entryDefinition).remove(entry.getRdn());
        engineContext.getEntryFilterCache(entry.getParent(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public void load(
            final Entry parent,
            final EntryDefinition entryDefinition,
            final SearchResults rdns,
            final SearchResults results)
            throws Exception {

        execute(new Runnable() {
            public void run() {
                try {
                    loadBackground(parent, entryDefinition, rdns, results);

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    results.close();
                }
            }
        });
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param rdns
     * @throws Exception
     */
    public void loadBackground(
            Entry parent,
            EntryDefinition entryDefinition,
            SearchResults rdns,
            SearchResults results
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading entry under "+parent.getDn()+" with rdns:");

        MRSWLock lock = getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            Collection rdnsToLoad = new TreeSet();

            String s = entryDefinition.getParameter(EntryDefinition.BATCH_SIZE);
            int batchSize = s == null ? EntryDefinition.DEFAULT_BATCH_SIZE : Integer.parseInt(s);

            while (rdns.hasNext()) {
                Row rdn = (Row)rdns.next();
                log.debug(" - "+rdn);

                Entry entry = (Entry)engineContext.getEntryDataCache(parent, entryDefinition).get(rdn);
                if (entry != null) {
                    entry.setParent(parent);
                    results.add(entry);
                    continue;
                }

                rdnsToLoad.add(rdn);

                if (rdnsToLoad.size() < batchSize) continue;

                SearchResults entries = joinEngine.load(parent, entryDefinition, rdnsToLoad);

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    entry = (Entry)i.next();
                    entry.setParent(parent);
                    results.add(entry);
                    engineContext.getEntryDataCache(parent, entryDefinition).put(entry.getRdn(), entry);
                }

                rdnsToLoad.clear();
            }

            if (rdnsToLoad.size() > 0) {
                SearchResults entries = joinEngine.load(parent, entryDefinition, rdnsToLoad);

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    entry.setParent(parent);
                    results.add(entry);
                    engineContext.getEntryDataCache(parent, entryDefinition).put(entry.getRdn(), entry);
                }
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }
    }

    public SearchResults search(
            PenroseConnection connection,
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter,
            SearchResults results
            ) throws Exception {

        SearchResults rdns = search(parent, entryDefinition, filter);
        load(parent, entryDefinition, rdns, results);

        return results;
    }

    public SearchResults search(
            final Entry parent,
            final EntryDefinition entryDefinition,
            final Filter filter)
            throws Exception {

        final SearchResults results = new SearchResults();

        execute(new Runnable() {
            public void run() {
                try {
                    searchBackground(parent, entryDefinition, filter, results);

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    results.close();
                }
            }
        });

        return results;
    }

    public void searchBackground(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter,
            SearchResults results
            ) throws Exception {

        try {
            log.debug("--------------------------------------------------------------------------------------");
            log.debug("Searching for entry "+entryDefinition.getDn()+" with filter "+filter);

            String dn = entryDefinition.getRdn()+","+parent.getDn();
            log.debug("Checking entry filter cache for ["+dn+"]");

            Collection rdns = engineContext.getEntryFilterCache(parent, entryDefinition).get(filter);

            if (rdns != null) {
                log.debug("Entry filter cache found: "+filter);
                results.addAll(rdns);
                return;
            }

            log.debug("Entry filter cache not found.");

            Map rows = searchEngine.search(parent, entryDefinition, filter);
            rdns = rows.keySet();

            engineContext.getEntryFilterCache(parent, entryDefinition).put(filter, rdns);

            filter = engineContext.getFilterTool().createFilter(rdns);
            engineContext.getEntryFilterCache(parent, entryDefinition).put(filter, rdns);

            results.addAll(rdns);

        } finally {
            results.close();
        }
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

    public String getStartingSourceName(EntryDefinition entryDefinition) throws Exception {

        log.debug("Searching the starting source for "+entryDefinition.getDn());

        Config config = engineContext.getConfig(entryDefinition.getDn());

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            for (Iterator j=relationship.getOperands().iterator(); j.hasNext(); ) {
                String operand = j.next().toString();

                int index = operand.indexOf(".");
                if (index < 0) continue;

                String sourceName = operand.substring(0, index);
                Source source = entryDefinition.getSource(sourceName);
                Source effectiveSource = config.getEffectiveSource(entryDefinition, sourceName);

                if (source == null && effectiveSource != null) {
                    log.debug("Source "+sourceName+" is defined in parent entry");
                    return sourceName;
                }

            }
        }

        Iterator i = entryDefinition.getSources().iterator();
        if (!i.hasNext()) return null;

        Source source = (Source)i.next();
        log.debug("Source "+source.getName()+" is the first defined in entry");
        return source.getName();
    }

    public Relationship getConnectingRelationship(EntryDefinition entryDefinition) throws Exception {

        log.debug("Searching the connecting relationship for "+entryDefinition.getDn());

        Config config = engineContext.getConfig(entryDefinition.getDn());

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            for (Iterator j=relationship.getOperands().iterator(); j.hasNext(); ) {
                String operand = j.next().toString();

                int index = operand.indexOf(".");
                if (index < 0) continue;

                String sourceName = operand.substring(0, index);
                Source source = entryDefinition.getSource(sourceName);
                Source effectiveSource = config.getEffectiveSource(entryDefinition, sourceName);

                if (source == null && effectiveSource != null) {
                    log.debug("Source "+sourceName+" is defined in parent entry");
                    return relationship;
                }

            }
        }

        return null;
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

    public Filter createFilter(Source source, Collection pks) throws Exception {

        Collection normalizedFilters = null;
        if (pks != null) {
            normalizedFilters = new TreeSet();
            for (Iterator i=pks.iterator(); i.hasNext(); ) {
                Row filter = (Row)i.next();

                Row f = new Row();
                for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    if (!name.startsWith(source.getName()+".")) continue;

                    String newName = name.substring(source.getName().length()+1);
                    f.set(newName, filter.get(name));
                }

                if (f.isEmpty()) continue;

                Row normalizedFilter = engineContext.getSchema().normalize(f);
                normalizedFilters.add(normalizedFilter);
            }
        }

        Filter filter = null;
        if (pks != null) {
            filter = engineContext.getFilterTool().createFilter(normalizedFilters);
        }

        return filter;
    }

    public EngineFilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(EngineFilterTool filterTool) {
        this.filterTool = filterTool;
    }

    public Filter generateFilter(Source toSource, Collection relationships, Collection rows) {
        log.debug("Generating filters:");

        Filter filter = null;
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            Filter subFilter = null;

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();

                String lhs = relationship.getLhs();
                String operator = relationship.getOperator();
                String rhs = relationship.getRhs();

                if (rhs.startsWith(toSource.getName()+".")) {
                    String exp = lhs;
                    lhs = rhs;
                    rhs = exp;
                }

                int index = lhs.indexOf(".");
                String name = lhs.substring(index+1);

                log.debug("   Filter: ("+name+" "+operator+" ?)");
                Object value = row.get(rhs);
                if (value == null) continue;

                SimpleFilter sf = new SimpleFilter(name, operator, value.toString());
                log.debug("   - "+rhs+" -> "+sf);

                if (subFilter == null) {
                    subFilter = sf;

                } else if (filter instanceof AndFilter) {
                    AndFilter af = (AndFilter)filter;
                    af.addFilterList(sf);

                } else {
                    AndFilter af = new AndFilter();
                    af.addFilterList(filter);
                    af.addFilterList(sf);
                    subFilter = af;
                }
            }

            if (filter == null) {
                filter = subFilter;

            } else if (filter instanceof OrFilter) {
                OrFilter of = (OrFilter)filter;
                of.addFilterList(subFilter);

            } else {
                OrFilter of = new OrFilter();
                of.addFilterList(filter);
                of.addFilterList(subFilter);
                filter = of;
            }
        }

        return filter;
    }
}

