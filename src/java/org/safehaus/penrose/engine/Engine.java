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
import org.safehaus.penrose.util.Formatter;
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

import javax.naming.directory.SearchResult;
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

    public Source getPrimarySource(EntryDefinition entryDefinition) throws Exception {
        Source source = (Source)primarySources.get(entryDefinition);
        if (source != null) return source;

        Config config = engineContext.getConfig(entryDefinition.getDn());
        entryDefinition = config.getParent(entryDefinition);
        if (entryDefinition == null) return null;

        return getPrimarySource(entryDefinition);
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

        //if (entryDefinition.getSources().isEmpty()) return null;

        Graph graph = new Graph();

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Collection sources = config.getEffectiveSources(entryDefinition);
        //if (sources.size() == 0) return null;

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            graph.addNode(source);
        }

        Collection relationships = config.getEffectiveRelationships(entryDefinition);
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
        getFieldValues("parent", parent == null ? null : parent.getDn(), sourceValues);

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

        engineContext.getEntryFilterCache(parent == null ? null : parent.getDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int delete(Entry entry) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();

        AttributeValues sourceValues = new AttributeValues();
        getFieldValues(entry.getDn(), sourceValues);

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        log.debug("Deleting entry "+entry.getDn()+" ["+sourceValues+"]");

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(engineContext, entryDefinition, sourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getEntryDataCache(entry.getParentDn(), entryDefinition).remove(entry.getRdn());
        engineContext.getEntryFilterCache(entry.getParentDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int modify(Entry entry, AttributeValues oldValues, AttributeValues newValues) throws Exception {

        //Entry parent = entry.getParent();
        EntryDefinition entryDefinition = entry.getEntryDefinition();
        Collection sources = entryDefinition.getSources();

        AttributeValues oldSourceValues = new AttributeValues();
        getFieldValues(entry.getDn(), oldSourceValues);

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

        log.debug("Modifying entry "+entryDefinition.getRdn()+","+entry.getParentDn()+" ["+oldSourceValues+"] with: "+newSourceValues);

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(engineContext, entryDefinition, oldSourceValues, newSourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        engineContext.getEntryDataCache(entry.getParentDn(), entryDefinition).remove(entry.getRdn());
        engineContext.getEntryFilterCache(entry.getParentDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public void load(
            final Collection parents,
            final EntryDefinition entryDefinition,
            final Collection rdns,
            final SearchResults results)
            throws Exception {

        final Entry parent = parents.isEmpty() ? null : (Entry)parents.iterator().next();
        final SearchResults batches = new SearchResults();

        execute(new Runnable() {
            public void run() {
                try {
                    createBatches(parent, entryDefinition, rdns, results, batches);

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    batches.close();
                }
            }
        });

        execute(new Runnable() {
            public void run() {
                try {
                    loadBatches(parents, entryDefinition, batches, results);

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    results.close();
                }
            }
        });
    }

    public void createBatches(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdns,
            SearchResults results,
            SearchResults batches
            ) throws Exception {

        Source primarySource = getPrimarySource(entryDefinition);

        Collection batch = new TreeSet();

        String s = entryDefinition.getParameter(EntryDefinition.BATCH_SIZE);
        int batchSize = s == null ? EntryDefinition.DEFAULT_BATCH_SIZE : Integer.parseInt(s);

        if (!rdns.isEmpty()) log.debug("Checking entry data cache:");

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            Entry entry = (Entry)engineContext.getEntryDataCache(parent == null ? entryDefinition.getParentDn() : parent.getDn(), entryDefinition).get(rdn);

            if (entry != null) {
                log.debug(" - "+rdn+" has been loaded");
                results.add(entry);
                continue;
            }

            Row filter = createFilter(primarySource, entryDefinition, rdn);
            log.debug(" - "+rdn+" has not been loaded, loading with filter "+filter);

            if (filter == null) continue;
            batch.add(filter);

            if (batch.size() < batchSize) continue;

            batches.add(batch);
            batch = new TreeSet();
        }

        if (!batch.isEmpty()) batches.add(batch);

        batches.close();
    }
    
    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param batches
     * @throws Exception
     */
    public void loadBatches(
            Collection parents,
            EntryDefinition entryDefinition,
            SearchResults batches,
            SearchResults results
            ) throws Exception {

        MRSWLock lock = getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            while (batches.hasNext()) {
                Collection keys = (Collection)batches.next();

                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("LOAD", 80));

                for (Iterator i=keys.iterator(); i.hasNext(); ) {
                    Row key = (Row)i.next();
                    log.debug(Formatter.displayLine(" - "+key, 80));
                }

                log.debug(Formatter.displaySeparator(80));

                SearchResults entries = joinEngine.load(parents, entryDefinition, keys);

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    results.add(entry);
                    engineContext.getEntryDataCache(entryDefinition.getParentDn(), entryDefinition).put(entry.getRdn(), entry);
                }
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }
    }

    public SearchResults search(
            PenroseConnection connection,
            Collection parents,
            EntryDefinition entryDefinition,
            Filter filter,
            SearchResults results
            ) throws Exception {

        Collection rdns = search(parents, entryDefinition, filter);

        if (rdns.isEmpty()) {
            results.close();
            return results;
        }

        load(parents, entryDefinition, rdns, results);

        return results;
    }

    public Collection search(
            Collection parents,
            EntryDefinition entryDefinition,
            Filter filter)
            throws Exception {

        Entry parent = parents.isEmpty() ? null : (Entry)parents.iterator().next();

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH", 80));
        log.debug(Formatter.displayLine(" - Entry: "+entryDefinition.getDn(), 80));
        log.debug(Formatter.displayLine(" - Filter: "+filter, 80));
        log.debug(Formatter.displaySeparator(80));

        String parentDn = parent == null ? entryDefinition.getParentDn() : parent.getDn();

        log.debug("Checking entry filter cache for "+filter);
        Collection rdns = engineContext.getEntryFilterCache(parentDn, entryDefinition).get(filter);

        if (rdns == null) {
            log.debug("Cache not found.");

            rdns = searchEngine.search(parents, entryDefinition, filter);

            engineContext.getEntryFilterCache(parent == null ? entryDefinition.getParentDn() : parent.getDn(), entryDefinition).put(filter, rdns);

            filter = engineContext.getFilterTool().createFilter(rdns);
            engineContext.getEntryFilterCache(parent == null ? entryDefinition.getParentDn() : parent.getDn(), entryDefinition).put(filter, rdns);

        } else {
            log.debug("Cache found: "+rdns);
        }

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH RESULTS", 80));

        if (rdns != null) {
            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();
                log.debug(Formatter.displayLine(" - "+rdn+","+entryDefinition.getParentDn(), 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));

        return rdns;
    }

    public void getFieldValues(String dn, AttributeValues results) throws Exception {
        getFieldValues(null, dn, results);
    }

    public void getFieldValues(String prefix, String dn, AttributeValues results) throws Exception {
        int i = dn.indexOf(",");
        String rdn = i < 0 ? dn : dn.substring(0, i);
        String parentDn = i < 0 ? null : dn.substring(i+1);

        i = rdn.indexOf("=");
        String attrName = rdn.substring(0, i);
        String attrValue = rdn.substring(i+1);
        Row row = new Row();
        row.set(attrName, attrValue);

        Config config = engineContext.getConfig(dn);
        EntryDefinition entryDefinition = config.findEntryDefinition(dn);
        Entry entry = (Entry)engineContext.getEntryDataCache(entryDefinition.getParentDn(), entryDefinition).get(row);

        if (parentDn != null) {
            getFieldValues((prefix == null ? "" : prefix+".")+"parent", parentDn, results);
        }

        log.debug(entry.getDn()+"'s source values:");

        Collection sources = entry.getSources();
        if (sources.size() == 0) return;

        AttributeValues sourceValues = entry.getSourceValues();
        if (sourceValues != null) {
            for (Iterator j=sourceValues.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
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

        // log.debug("Searching the connecting relationship for "+entryDefinition.getDn());

        Config config = engineContext.getConfig(entryDefinition.getDn());

        Collection relationships = config.getEffectiveRelationships(entryDefinition);

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            // log.debug(" - checking "+relationship);

            String lhs = relationship.getLhs();
            String rhs = relationship.getRhs();

            int lindex = lhs.indexOf(".");
            String lsourceName = lindex < 0 ? lhs : lhs.substring(0, lindex);
            Source lsource = entryDefinition.getSource(lsourceName);

            int rindex = rhs.indexOf(".");
            String rsourceName = rindex < 0 ? rhs : rhs.substring(0, rindex);
            Source rsource = entryDefinition.getSource(rsourceName);

            if (lsource == null && rsource != null || lsource != null && rsource == null) {
                return relationship;
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

    public EngineFilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(EngineFilterTool filterTool) {
        this.filterTool = filterTool;
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

    public Row createFilter(Source source, EntryDefinition entryDefinition, Row rdn) throws Exception {

        log.debug(" - "+rdn);

        if (source == null) {
            return new Row();
        }

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Collection fields = config.getSearchableFields(source);

        Interpreter interpreter = engineContext.newInterpreter();
        interpreter.set(rdn);

        Row filter = new Row();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            Field field = (Field)j.next();
            String name = field.getName();

            Expression expression = field.getExpression();
            if (expression == null) continue;

            Object value = interpreter.eval(expression);
            if (value == null) continue;

            log.debug("   ==> Filter: "+source.getName()+"."+field.getName()+"="+value);
            //filter.set(source.getName()+"."+name, value);
            filter.set(name, value);
        }

        //if (filter.isEmpty()) return null;

        return filter;
    }

    public Filter generateFilter(Source source, Collection relationships, Collection rows) throws Exception {
        log.debug("Generating filters for source "+source.getName());

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

                if (rhs.startsWith(source.getName()+".")) {
                    String exp = lhs;
                    lhs = rhs;
                    rhs = exp;
                }

                int index = lhs.indexOf(".");
                String name = lhs.substring(index+1);

                log.debug("   - "+rhs+" ==> ("+name+" "+operator+" ?)");
                Object value = row.get(rhs);
                if (value == null) continue;

                SimpleFilter sf = new SimpleFilter(name, operator, value.toString());
                log.debug("     --> "+sf);

                subFilter = engineContext.getFilterTool().appendAndFilter(subFilter, sf);
            }

            filter = engineContext.getFilterTool().appendOrFilter(filter, subFilter);
        }

        return filter;
    }

    public void createEntries(
            EntryDefinition entryDefinition,
            AttributeValues sourceValues,
            SearchResults results)
            throws Exception {

        Interpreter interpreter = engineContext.newInterpreter();
        interpreter.set(sourceValues);

        Row rdn = new Row();
        Collection attributes = entryDefinition.getAttributeDefinitions();

        AttributeValues attributeValues = new AttributeValues();

        for (Iterator j=attributes.iterator(); j.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)j.next();

            String name = attribute.getName();
            Expression expression = attribute.getExpression();
            if (expression == null) continue;

            Object value = interpreter.eval(expression);
            if (value == null) {
                if (attribute.isRdn()) return;
                continue;
            }

            if (attribute.isRdn()) rdn.set(name, value);

            attributeValues.add(name, value);
        }

        Collection dns = computeParentDns(entryDefinition, interpreter, rdn.toString());

        for (Iterator i=dns.iterator(); i.hasNext(); ) {
            String dn = (String)i.next();

            Entry entry = new Entry(dn, entryDefinition, sourceValues, attributeValues);
            
            results.add(entry);
            log.debug("Entry:\n"+entry);
        }
    }
    
    public Collection computeParentDns(EntryDefinition entryDefinition, Interpreter interpreter, String dnPrefix) throws Exception {
        Config config = engineContext.getConfig(entryDefinition.getDn());
        EntryDefinition parentDefinition = config.getParent(entryDefinition);

        Collection results = new ArrayList();
        computeParentDns(parentDefinition, interpreter, dnPrefix, results);

        return results;
    }

    public void computeParentDns(EntryDefinition entryDefinition, Interpreter interpreter, String dnPrefix, Collection results) throws Exception {

        Collection attributeDefinitions = entryDefinition.getAttributeDefinitions();
        AttributeValues av = new AttributeValues();

        for (Iterator j=attributeDefinitions.iterator(); j.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)j.next();
            if (!attribute.isRdn()) continue;

            String name = attribute.getName();
            Expression expression = attribute.getExpression();
            if (expression == null) continue;

            Object value = interpreter.eval(expression);
            if (value == null) continue;

            av.add(name, value);
        }

        Config config = engineContext.getConfig(entryDefinition.getDn());
        EntryDefinition parentDefinition = config.getParent(entryDefinition);

        Collection rows = engineContext.getTransformEngine().convert(av);
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            String newDnPrefix = dnPrefix+","+row;

            if (parentDefinition == null) {
                String dn = newDnPrefix +(entryDefinition.getParentDn() == null ? "" : ","+entryDefinition.getParentDn());
                results.add(dn);
            } else {
                computeParentDns(parentDefinition, interpreter, newDnPrefix, results);
            }
        }
    }

}

