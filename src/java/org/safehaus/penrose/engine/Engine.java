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
import org.safehaus.penrose.schema.SchemaReader;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.EngineCache;
import org.safehaus.penrose.cache.ConnectorCache;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.config.ServerConfigReader;
import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.config.ConfigReader;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphEdge;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Engine {

    static Logger log = Logger.getLogger(Engine.class);

    private EngineConfig engineConfig;

    private Map graphs = new HashMap();
    private Map primarySources = new HashMap();

    private Map locks = new HashMap();
    private Queue queue = new Queue();

    private ThreadPool threadPool;

    private boolean stopping = false;

    private InterpreterFactory interpreterFactory;
    private Schema schema;
    private Connector connector;

    private EngineFilterTool filterTool;
    private SearchEngine searchEngine;
    private LoadEngine loadEngine;
    private MergeEngine mergeEngine;
    private JoinEngine joinEngine;
    private TransformEngine transformEngine;

    private Map configs = new TreeMap();
    private Map caches = new TreeMap();

    /**
     * Initialize the engine with a Penrose instance
     *
     * @throws Exception
     */
    public void init(EngineConfig engineConfig) throws Exception {
        this.engineConfig = engineConfig;

        log.debug("-------------------------------------------------");
        log.debug("Initializing "+engineConfig.getEngineName()+" engine ...");

        filterTool = new EngineFilterTool(this);

        searchEngine = new SearchEngine(this);
        loadEngine = new LoadEngine(this);
        mergeEngine = new MergeEngine(this);
        joinEngine = new JoinEngine(this);
        transformEngine = new TransformEngine(this);

        String s = engineConfig.getParameter(EngineConfig.THREAD_POOL_SIZE);
        int threadPoolSize = s == null ? EngineConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        threadPool = new ThreadPool(threadPoolSize);
    }

    public void start() throws Exception {
        execute(new RefreshThread(this));
    }

    public void addConfig(Config config) throws Exception {

        for (Iterator i=config.getRootEntryDefinitions().iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            String ndn = schema.normalize(entryDefinition.getDn());
            configs.put(ndn, config);

            analyze(entryDefinition);
        }
    }

    public Config getConfig(Source source) throws Exception {
        String connectionName = source.getConnectionName();
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            if (config.getConnectionConfig(connectionName) != null) return config;
        }
        return null;
    }

    public Config getConfig(String dn) throws Exception {
        String ndn = schema.normalize(dn);
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            if (config.getEntryDefinition(dn) != null) return config;
        }
/*
        for (Iterator i=configs.keySet().iterator(); i.hasNext(); ) {
            String suffix = (String)i.next();
            if (ndn.endsWith(suffix)) return (Config)configs.get(suffix);
        }
*/
        return null;
    }

    public void analyze(EntryDefinition entryDefinition) throws Exception {

        //log.debug("Entry "+entryDefinition.getDn()+":");

        Source source = computePrimarySource(entryDefinition);
        if (source != null) {
            primarySources.put(entryDefinition, source);
            //log.debug(" - primary source: "+source);
        }

        Graph graph = computeGraph(entryDefinition);
        if (graph != null) {
            graphs.put(entryDefinition, graph);
            //log.debug(" - graph: "+graph);
        }

        Config config = getConfig(entryDefinition.getDn());
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
        return source;
/*
        if (source != null) return source;

        Config config = getConfig(entryDefinition.getDn());
        entryDefinition = config.getParent(entryDefinition);
        if (entryDefinition == null) return null;

        return getPrimarySource(entryDefinition);
*/
    }

    Source computePrimarySource(EntryDefinition entryDefinition) throws Exception {

        Collection rdnAttributes = entryDefinition.getRdnAttributes();

        // TODO need to handle multiple rdn attributes
        AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();

        if (rdnAttribute.getConstant() == null) {
            String variable = rdnAttribute.getVariable();
            if (variable != null) {
                int i = variable.indexOf(".");
                String sourceName = variable.substring(0, i);
                Source source = entryDefinition.getSource(sourceName);
                return source;
            }

            Expression expression = rdnAttribute.getExpression();
            String foreach = expression.getForeach();
            if (foreach != null) {
                int i = foreach.indexOf(".");
                String sourceName = foreach.substring(0, i);
                Source source = entryDefinition.getSource(sourceName);
                return source;
            }

            Interpreter interpreter = interpreterFactory.newInstance();

            Collection variables = interpreter.parseVariables(expression.getScript());

            for (Iterator i=variables.iterator(); i.hasNext(); ) {
                String sourceName = (String)i.next();
                Source source = entryDefinition.getSource(sourceName);
                if (source != null) return source;
            }

            interpreter.clear();
        }

        Collection sources = entryDefinition.getSources();
        if (sources.isEmpty()) return null;

        return (Source)sources.iterator().next();
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

        //log.debug("Graph for "+entryDefinition.getDn()+":");

        //if (entryDefinition.getSources().isEmpty()) return null;

        Graph graph = new Graph();

        Config config = getConfig(entryDefinition.getDn());
        Collection sources = config.getEffectiveSources(entryDefinition);
        //if (sources.size() == 0) return null;

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            graph.addNode(source);
        }

        Collection relationships = config.getEffectiveRelationships(entryDefinition);
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            //log.debug("Checking ["+relationship.getExpression()+"]");

            String lhs = relationship.getLhs();
            int lindex = lhs.indexOf(".");
            if (lindex < 0) continue;

            String lsourceName = lhs.substring(0, lindex);
            Source lsource = config.getEffectiveSource(entryDefinition, lsourceName);
            if (lsource == null) continue;

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            if (rindex < 0) continue;

            String rsourceName = rhs.substring(0, rindex);
            Source rsource = config.getEffectiveSource(entryDefinition, rsourceName);
            if (rsource == null) continue;

            Set nodes = new HashSet();
            nodes.add(lsource);
            nodes.add(rsource);

            GraphEdge edge = graph.getEdge(nodes);
            if (edge == null) {
                edge = new GraphEdge(lsource, rsource, new ArrayList());
                graph.addEdge(edge);
            }

            Collection list = (Collection)edge.getObject();
            list.add(relationship);
        }

        Collection edges = graph.getEdges();
        for (Iterator i=edges.iterator(); i.hasNext(); ) {
            GraphEdge edge = (GraphEdge)i.next();
            //log.debug(" - "+edge);
        }

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

    public int bind(Entry entry, String password) throws Exception {

        log.debug("Bind as user "+entry.getDn());

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues attributeValues = entry.getAttributeValues();

        Collection set = attributeValues.get("userPassword");

        if (set != null) {
            for (Iterator i = set.iterator(); i.hasNext(); ) {
                String userPassword = (String)i.next();
                log.debug("userPassword: "+userPassword);
                if (PasswordUtil.comparePassword(password, userPassword)) return LDAPException.SUCCESS;
            }
        }

        Collection sources = entryDefinition.getSources();
        Config config = getConfig(entryDefinition.getDn());

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
            SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

            Map entries = transformEngine.split(source, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+": "+sourceValues);

                int rc = connector.bind(sourceDefinition, entryDefinition, sourceValues, password);
                if (rc == LDAPException.SUCCESS) return rc;
            }
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public int add(
            Entry parent,
            EntryDefinition entryDefinition,
            AttributeValues attributeValues)
            throws Exception {

        AttributeValues sourceValues = parent.getSourceValues();

        Collection sources = entryDefinition.getSources();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            Row pk = transformEngine.translate(source, attributeValues, output);
            if (pk == null) continue;

            log.debug(" - "+pk+": "+output);
            for (Iterator j=output.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = output.get(name);
                sourceValues.add(source.getName()+"."+name, values);
            }
        }

        log.debug("Adding entry "+entryDefinition.getRdn()+","+parent.getDn()+" with values: "+sourceValues);

        Config config = getConfig(entryDefinition.getDn());

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);
        String startingSourceName = getStartingSourceName(entryDefinition);
        if (startingSourceName == null) return LDAPException.SUCCESS;

        Source startingSource = config.getEffectiveSource(entryDefinition, startingSourceName);
        log.debug("Starting from source: "+startingSourceName);

        Collection relationships = graph.getEdgeObjects(startingSource);
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Collection list = (Collection)i.next();

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
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
        }

        AddGraphVisitor visitor = new AddGraphVisitor(this, entryDefinition, sourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        getCache(parent == null ? null : parent.getDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int delete(Entry entry) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();

        AttributeValues sourceValues = entry.getSourceValues();
        //getFieldValues(entry.getDn(), sourceValues);

        Graph graph = getGraph(entryDefinition);
        Source primarySource = getPrimarySource(entryDefinition);

        log.debug("Deleting entry "+entry.getDn()+" ["+sourceValues+"]");

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(this, entryDefinition, sourceValues);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        getCache(entry.getParentDn(), entryDefinition).remove(entry.getRdn());
        getCache(entry.getParentDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int modrdn(Entry entry, String newRdn) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldAttributeValues = entry.getAttributeValues();
        AttributeValues oldSourceValues = entry.getSourceValues();

        Row rdn = Entry.getRdn(newRdn);

        AttributeValues newAttributeValues = new AttributeValues(oldAttributeValues);
        Collection rdnAttributes = entryDefinition.getRdnAttributes();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            String name = attributeDefinition.getName();
            String newValue = (String)rdn.get(name);

            newAttributeValues.remove(name);
            newAttributeValues.add(name, newValue);
        }

        AttributeValues newSourceValues = new AttributeValues(oldSourceValues);
        Collection sources = entryDefinition.getSources();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            transformEngine.translate(source, newAttributeValues, output);
            newSourceValues.set(source.getName(), output);
        }

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("MODRDN", 80));
        log.debug(Formatter.displayLine("Entry: "+entry.getDn(), 80));
        log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));

        log.debug(Formatter.displayLine("Attribute values:", 80));
        for (Iterator iterator = newAttributeValues.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            Collection values = newAttributeValues.get(name);
            log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
        }

        log.debug(Formatter.displayLine("Old source values:", 80));
        for (Iterator iterator = oldSourceValues.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            Collection values = oldSourceValues.get(name);
            log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
        }

        log.debug(Formatter.displayLine("New source values:", 80));
        for (Iterator iterator = newSourceValues.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            Collection values = newSourceValues.get(name);
            log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
        }

        log.debug(Formatter.displaySeparator(80));

        ModRdnGraphVisitor visitor = new ModRdnGraphVisitor(this, entryDefinition, oldSourceValues, newSourceValues);
        visitor.run();

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        getCache(entry.getParentDn(), entryDefinition).remove(entry.getRdn());
        getCache(entry.getParentDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public int modify(Entry entry, AttributeValues oldValues, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldSourceValues = entry.getSourceValues();

        AttributeValues newSourceValues = (AttributeValues)oldSourceValues.clone();
        Collection sources = entryDefinition.getSources();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            transformEngine.translate(source, newValues, output);
            newSourceValues.set(source.getName(), output);
        }

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("MODIFY", 80));
        log.debug(Formatter.displayLine("Entry: "+entry.getDn(), 80));

        log.debug(Formatter.displayLine("Old source values:", 80));
        for (Iterator iterator = oldSourceValues.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            Collection values = oldSourceValues.get(name);
            log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
        }

        log.debug(Formatter.displayLine("New source values:", 80));
        for (Iterator iterator = newSourceValues.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            Collection values = newSourceValues.get(name);
            log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
        }

        log.debug(Formatter.displaySeparator(80));

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(this, entryDefinition, oldSourceValues, newSourceValues);
        visitor.run();

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        getCache(entry.getParentDn(), entryDefinition).remove(entry.getRdn());
        getCache(entry.getParentDn(), entryDefinition).invalidate();

        return LDAPException.SUCCESS;
    }

    public String getParentSourceValues(Collection path, EntryDefinition entryDefinition, AttributeValues parentSourceValues) throws Exception {
        Config config = getConfig(entryDefinition.getDn());
        EntryDefinition parentDefinition = config.getParent(entryDefinition);

        String prefix = null;
        Interpreter interpreter = interpreterFactory.newInstance();

        //log.debug("Parents' source values:");
        for (Iterator iterator = path.iterator(); iterator.hasNext(); ) {
            Map map = (Map)iterator.next();
            String dn = (String)map.get("dn");
            Entry entry = (Entry)map.get("entry");
            //log.debug(" - "+dn);

            prefix = prefix == null ? "parent" : "parent."+prefix;

            if (entry == null) {
                AttributeValues av = computeAttributeValues(parentDefinition, interpreter);
                for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = av.get(name);
                    name = prefix+"."+name;
                    //log.debug("   - "+name+": "+values);
                    parentSourceValues.add(name, values);
                }
                interpreter.clear();

            } else {
                AttributeValues av = entry.getAttributeValues();
                for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = av.get(name);
                    name = prefix+"."+name;
                    //log.debug("   - "+name+": "+values);
                    parentSourceValues.add(name, values);
                }

                AttributeValues sv = entry.getSourceValues();
                for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = sv.get(name);
                    if (name.startsWith("parent.")) name = prefix+"."+name;
                    //log.debug("   - "+name+": "+values);
                    parentSourceValues.add(name, values);
                }
            }

            parentDefinition = config.getParent(parentDefinition);
        }

        return prefix;
    }

    /**
     * Check whether all rdn attributes are either constant or unique variable, or use all primary keys from one source.
     */
    public boolean isUnique(EntryDefinition entryDefinition) throws Exception {

        Collection sources = new TreeSet();
        Collection fields = new TreeSet();

        Collection attributeDefinitions = entryDefinition.getRdnAttributes();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            if (attributeDefinition.getConstant() != null) continue;

            String variable = attributeDefinition.getVariable();

            if (variable != null) { // get the source and field name
                int j = variable.indexOf(".");
                String sourceAlias = variable.substring(0, j);
                String fieldName = variable.substring(j+1);

                sources.add(sourceAlias);
                fields.add(fieldName);

                continue;
            }

            // attribute is an expression
            return false;
        }

        //log.debug("RDN sources: "+sources);

        // rdn is constant
        if (sources.isEmpty()) return true;

        // rdn uses more than one source
        if (sources.size() > 1) return false;

        String sourceAlias = (String)sources.iterator().next();
        Source source = entryDefinition.getSource(sourceAlias);

        Config config = getConfig(entryDefinition.getDn());
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection uniqueFields = new TreeSet();
        Collection pkFields = new TreeSet();

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();
            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(fieldName);

            if (fieldDefinition.isUnique()) {
                uniqueFields.add(fieldName);
                continue;
            }

            if (fieldDefinition.isPrimaryKey()) {
                pkFields.add(fieldName);
                continue;
            }

            return false;
        }

        //log.debug("RDN unique fields: "+uniqueFields);
        //log.debug("RDN PK fields: "+pkFields);

        // rdn uses unique fields
        if (pkFields.isEmpty() && !uniqueFields.isEmpty()) return true;

        Collection list = sourceDefinition.getPrimaryKeyNames();
        //log.debug("Source PK fields: "+list);

        // rdn uses primary key fields
        boolean result = pkFields.equals(list);

        EntryDefinition parentDefinition = config.getParent(entryDefinition);
        if (parentDefinition == null) return result;

        return isUnique(parentDefinition);
    }

    public Entry find(
            Collection path,
            EntryDefinition entryDefinition
            ) throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("FIND", 80));
        log.debug(Formatter.displayLine("Entry: "+entryDefinition.getDn(), 80));
        log.debug(Formatter.displayLine("Parents:", 80));

        for (Iterator i=path.iterator(); i.hasNext(); ) {
            Map map = (Map)i.next();
            String dn = (String)map.get("dn");
            log.debug(Formatter.displayLine(" - "+dn, 80));
        }

        log.debug(Formatter.displaySeparator(80));

        SearchResults entries = new SearchResults();

        AttributeValues parentSourceValues = new AttributeValues();
        String prefix = getParentSourceValues(path, entryDefinition, parentSourceValues);

        SearchResults results = search(path, parentSourceValues, entryDefinition, null, null);

        if (results.size() == 0) return null;

        Entry entry = (Entry)results.next();

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("FIND RESULT", 80));
        log.debug(Formatter.displayLine("dn: "+entry.getDn(), 80));
        log.debug(Formatter.displaySeparator(80));

        return entry;
    }

    public SearchResults search(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryDefinition entryDefinition,
            final Filter filter,
            Collection attributeNames) throws Exception {

        Collection attributeDefinitions = entryDefinition.getAttributeDefinitions(attributeNames);
        //log.debug("Attribute definitions: "+attributeDefinitions);

        // check if client only requests the dn to be returned
        final boolean dnOnly = attributeNames != null && attributeNames.contains("dn")
                && attributeDefinitions.isEmpty()
                && "(objectclass=*)".equals(filter.toString().toLowerCase());

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH", 80));
        log.debug(Formatter.displayLine("Entry: "+entryDefinition.getDn(), 80));
        log.debug(Formatter.displayLine("Filter: "+filter, 80));
        log.debug(Formatter.displayLine("Parents:", 80));

        if (parentSourceValues != null) {
            for (Iterator i = parentSourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection values = parentSourceValues.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));

        final SearchResults dns = new SearchResults();
        final SearchResults results = new SearchResults();

        Interpreter interpreter = getInterpreterFactory().newInstance();

        String s = engineConfig.getParameter(EngineConfig.ALLOW_CONCURRENCY);
        boolean allowConcurrency = s == null ? true : new Boolean(s).booleanValue();

        Config config = getConfig(entryDefinition.getDn());
        Collection sources = entryDefinition.getSources();
        log.debug("Sources: "+sources);
        Collection effectiveSources = config.getEffectiveSources(entryDefinition);
        log.debug("Effective Sources: "+effectiveSources);

        if (sources.size() == 0 && effectiveSources.size() == 0) {

            Collection list = computeDns(interpreter, entryDefinition, parentSourceValues);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                String dn = (String)j.next();
                log.debug(" - "+dn);

                Map map = new HashMap();
                map.put("dn", entryDefinition.getDn());
                map.put("sourceValues", parentSourceValues);
                dns.add(map);
            }
            dns.close();

        } else if (sources.size() == 1 && effectiveSources.size() == 1) {
            if (allowConcurrency) {
                execute(new Runnable() {
                    public void run() {
                        try {
                            searchEngine.simpleSearch(parentSourceValues, entryDefinition, filter, dns);

                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            dns.setReturnCode(LDAPException.OPERATIONS_ERROR);
                        }
                    }
                });
            } else {
                searchEngine.simpleSearch(parentSourceValues, entryDefinition, filter, dns);
            }

        } else {

            if (allowConcurrency) {
                execute(new Runnable() {
                    public void run() {
                        try {
                            searchEngine.search(path, parentSourceValues, entryDefinition, filter, dns);

                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            dns.setReturnCode(LDAPException.OPERATIONS_ERROR);
                        }
                    }
                });
            } else {
                searchEngine.search(path, parentSourceValues, entryDefinition, filter, dns);
            }
        }

        if (dnOnly) {
           for (Iterator i=dns.iterator(); i.hasNext(); ) {
               Map map = (Map)i.next();
               String dn = (String)map.get("dn");
               AttributeValues sv = (AttributeValues)map.get("sourceValues");

               AttributeValues attributeValues = computeAttributeValues(entryDefinition, sv, interpreter);
               Entry entry = new Entry(dn, entryDefinition, sv, attributeValues);

               results.add(entry);
           }
           return results;
        }

        final Interpreter batchInterpreter = interpreterFactory.newInstance();
        final SearchResults batches = new SearchResults();
        final SearchResults loadedBatches = new SearchResults();

        if (sources.size() == 0 && effectiveSources.size() == 0 || sources.size() == 1 && effectiveSources.size() == 1) {

            if (allowConcurrency) {
                execute(new Runnable() {
                    public void run() {
                        try {
                            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                                Map map = (Map)i.next();
                                loadedBatches.add(map);
                            }
                            loadedBatches.close();

                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            batches.setReturnCode(LDAPException.OPERATIONS_ERROR);
                        }
                    }
                });
            } else {
                for (Iterator i=dns.iterator(); i.hasNext(); ) {
                    Map map = (Map)i.next();
                    loadedBatches.add(map);
                }
                loadedBatches.close();
            }

        } else {
            if (allowConcurrency) {
                execute(new Runnable() {
                    public void run() {
                        try {
                            createBatches(batchInterpreter, entryDefinition, dns, results, batches);

                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            batches.setReturnCode(LDAPException.OPERATIONS_ERROR);
                        }
                    }
                });
            } else {
                createBatches(batchInterpreter, entryDefinition, dns, results, batches);
            }

            if (allowConcurrency) {
                execute(new Runnable() {
                    public void run() {
                        try {
                            loadEngine.load(entryDefinition, batches, loadedBatches);

                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            loadedBatches.setReturnCode(LDAPException.OPERATIONS_ERROR);
                        }
                    }
                });
            } else {
                loadEngine.load(entryDefinition, batches, loadedBatches);
            }

        }

        final Interpreter mergeInterpreter = interpreterFactory.newInstance();

        if (allowConcurrency) {
            execute(new Runnable() {
                public void run() {
                    try {
                        mergeEngine.merge(entryDefinition, loadedBatches, mergeInterpreter, results);

                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    }
                }
            });
        } else {
            mergeEngine.merge(entryDefinition, loadedBatches, mergeInterpreter, results);
        }

        return results;
    }

    public void createBatches(
            Interpreter interpreter,
            EntryDefinition entryDefinition,
            SearchResults entries,
            SearchResults results,
            SearchResults batches
            ) throws Exception {

        try {
            Config config = getConfig(entryDefinition.getDn());
            Source primarySource = getPrimarySource(entryDefinition);

            Collection batch = new ArrayList();

            String s = entryDefinition.getParameter(EntryDefinition.BATCH_SIZE);
            int batchSize = s == null ? EntryDefinition.DEFAULT_BATCH_SIZE : Integer.parseInt(s);

            for (Iterator i=entries.iterator(); i.hasNext(); ) {
                Map map = (Map)i.next();
                String dn = (String)map.get("dn");
                AttributeValues sv = (AttributeValues)map.get("sourceValues");

                Row rdn = Entry.getRdn(dn);

                if (config.getParent(entryDefinition) != null) {
                    String parentDn = Entry.getParentDn(dn);

                    log.debug("Checking "+rdn+" in entry data cache for "+parentDn);
                    Entry entry = (Entry)getCache(parentDn, entryDefinition).get(rdn);

                    if (entry != null) {
                        log.debug(" - "+rdn+" has been loaded");
                        results.add(entry);
                        continue;
                    }
                }

                Row filter = createFilter(interpreter, primarySource, entryDefinition, rdn);
                if (filter == null) continue;

                //if (filter.isEmpty()) filter.add(rdn);

                log.debug("- "+rdn+" has not been loaded, loading with key "+filter);
                map.put("filter", filter);
                batch.add(map);

                if (batch.size() < batchSize) continue;

                batches.add(batch);
                batch = new ArrayList();
            }

            if (!batch.isEmpty()) batches.add(batch);

        } finally {
            batches.close();
        }
    }

    public String getStartingSourceName(EntryDefinition entryDefinition) throws Exception {

        log.debug("Searching the starting source for "+entryDefinition.getDn());

        Config config = getConfig(entryDefinition.getDn());

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

        Config config = getConfig(entryDefinition.getDn());

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

    public MergeEngine getMergeEngine() {
        return mergeEngine;
    }

    public void setJoinEngine(MergeEngine mergeEngine) {
        this.mergeEngine = mergeEngine;
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

                Row normalizedFilter = schema.normalize(f);
                normalizedFilters.add(normalizedFilter);
            }
        }

        Filter filter = null;
        if (pks != null) {
            filter = FilterTool.createFilter(normalizedFilters);
        }

        return filter;
    }

    public Row createFilter(
            Interpreter interpreter,
            Source source,
            EntryDefinition entryDefinition,
            Row rdn) throws Exception {

        if (source == null) {
            return new Row();
        }

        Config config = getConfig(entryDefinition.getDn());
        Collection fields = config.getSearchableFields(source);

        interpreter.set(rdn);

        Row filter = new Row();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            Field field = (Field)j.next();
            String name = field.getName();

            Object value = interpreter.eval(field);
            if (value == null) continue;

            //log.debug("   ==> "+field.getName()+"="+value);
            //filter.set(source.getName()+"."+name, value);
            filter.set(name, value);
        }

        //if (filter.isEmpty()) return null;

        interpreter.clear();

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

                subFilter = FilterTool.appendAndFilter(subFilter, sf);
            }

            filter = FilterTool.appendOrFilter(filter, subFilter);
        }

        return filter;
    }

    public Filter generateFilter(Source toSource, Collection relationships, AttributeValues av) throws Exception {
        log.debug("Filters:");

        Filter filter = null;

        for (Iterator j=relationships.iterator(); j.hasNext(); ) {
            Relationship relationship = (Relationship)j.next();
            log.debug(" - "+relationship);

            String lhs = relationship.getLhs();
            String operator = relationship.getOperator();
            String rhs = relationship.getRhs();

            if (rhs.startsWith(toSource.getName()+".")) {
                String exp = lhs;
                lhs = rhs;
                rhs = exp;
            }

            int lindex = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, lindex);
            String lname = lhs.substring(lindex+1);

            int rindex = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, rindex);
            String rname = rhs.substring(rindex+1);

            //log.debug("   converting "+rhs+" ==> ("+lname+" "+operator+" ?)");

            Collection v = av.get(rhs);
            log.debug("   - found "+v);
            if (v == null) continue;

            Filter orFilter = null;
            for (Iterator k=v.iterator(); k.hasNext(); ) {
                Object value = k.next();

                SimpleFilter sf = new SimpleFilter(lname, operator, value.toString());
                //log.debug("   - "+sf);

                orFilter = FilterTool.appendOrFilter(orFilter, sf);
            }

            filter = FilterTool.appendAndFilter(filter, orFilter);
        }

        return filter;
    }

    public Collection computeDns(
            Interpreter interpreter,
            EntryDefinition entryDefinition,
            AttributeValues sourceValues)
            throws Exception {

        interpreter.set(sourceValues);

        Collection results = new ArrayList();

        results.add(computeDns(interpreter, entryDefinition));

        interpreter.clear();

        return results;
    }

    public String computeDns(Interpreter interpreter, EntryDefinition entryDefinition) throws Exception {

        Row rdn = computeRdn(interpreter, entryDefinition);

        Config config = getConfig(entryDefinition.getDn());
        EntryDefinition parentDefinition = config.getParent(entryDefinition);

        String parentDn;
        if (parentDefinition != null) {
            parentDn = computeDns(interpreter, parentDefinition);
        } else {
            parentDn = entryDefinition.getParentDn();
        }

        return rdn +(parentDn == null ? "" : ","+parentDn);
    }

    public Row computeRdn(
            Interpreter interpreter,
            EntryDefinition entryDefinition
            ) throws Exception {

        Row rdn = new Row();

        Collection rdnAttributes = entryDefinition.getRdnAttributes();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            String name = attributeDefinition.getName();

            Object value = interpreter.eval(attributeDefinition);
            if (value == null) return null;

            rdn.set(name, value);
        }

        return rdn;
    }

    public AttributeValues computeAttributeValues(
            EntryDefinition entryDefinition,
            Interpreter interpreter
            ) throws Exception {

        return computeAttributeValues(entryDefinition, null, interpreter);
    }

    public AttributeValues computeAttributeValues(
            EntryDefinition entryDefinition,
            AttributeValues sourceValues,
            Interpreter interpreter
            ) throws Exception {

        AttributeValues attributeValues = new AttributeValues();

        if (sourceValues != null) interpreter.set(sourceValues);

        Collection attributeDefinitions = entryDefinition.getAttributeDefinitions();
        for (Iterator j=attributeDefinitions.iterator(); j.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)j.next();

            Object value = interpreter.eval(attributeDefinition);
            if (value == null) continue;

            String name = attributeDefinition.getName();
            attributeValues.add(name, value);
        }

        interpreter.clear();

        Collection objectClasses = entryDefinition.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributeValues.add("objectClass", objectClass);
        }

        return attributeValues;
    }

    public LoadEngine getLoadEngine() {
        return loadEngine;
    }

    public void setLoadEngine(LoadEngine loadEngine) {
        this.loadEngine = loadEngine;
    }

    public JoinEngine getJoinEngine() {
        return joinEngine;
    }

    public void setJoinEngine(JoinEngine joinEngine) {
        this.joinEngine = joinEngine;
    }

    public EngineConfig getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public EngineCache getCache(String parentDn, EntryDefinition entryDefinition) throws Exception {
        String cacheName = entryDefinition.getParameter(EntryDefinition.CACHE);
        cacheName = cacheName == null ? EntryDefinition.DEFAULT_CACHE : cacheName;
        CacheConfig cacheConfig = engineConfig.getCacheConfig(EngineConfig.CACHE);

        String key = entryDefinition.getRdn()+","+parentDn;

        EngineCache cache = (EngineCache)caches.get(key);

        if (cache == null) {

            String cacheClass = cacheConfig.getCacheClass();
            cacheClass = cacheClass == null ? CacheConfig.DEFAULT_ENGINE_CACHE : cacheClass;

            Class clazz = Class.forName(cacheClass);
            cache = (EngineCache)clazz.newInstance();

            cache.setParentDn(parentDn);
            cache.setEntryDefinition(entryDefinition);
            cache.setEngine(this);
            cache.init(cacheConfig);

            caches.put(key, cache);
        }

        return cache;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public static void main(String args[]) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: org.safehaus.penrose.engine.Engine [command]");
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

        InterpreterConfig interpreterConfig = serverCfg.getInterpreterConfig();
        InterpreterFactory intFactory = new InterpreterFactory(interpreterConfig);

        Schema schm = createSchema(homeDirectory);
        Collection cfgs = getConfigs(homeDirectory);
        Connector cntr = createConnector(serverCfg, cfgs, command);
        Engine engine = createEngine(serverCfg, intFactory, schm, cntr, cfgs, command);
        //engine.start();

        if ("create".equals(command)) {
            engine.create();

        } else if ("load".equals(command)) {
            engine.load();

        } else if ("clean".equals(command)) {
            engine.clean();

        } else if ("drop".equals(command)) {
            engine.drop();

        } else if ("run".equals(command)) {
            engine.start();
        }

        engine.stop();
    }

    public static Schema createSchema(String homeDirectory) throws Exception {
        SchemaReader reader = new SchemaReader();
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema");
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema"+File.separator+"ext");
        return reader.getSchema();
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

        Connector cntr = new Connector();
        cntr.init(serverCfg, connectorCfg);

        for (Iterator i=cfgs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            cntr.addConfig(config);
        }

        if ("create".equals(command)) {
            cntr.create();

        } else if ("load".equals(command)) {
            cntr.load();

        } else if ("clean".equals(command)) {
            cntr.clean();

        } else if ("drop".equals(command)) {
            cntr.drop();

        } else if ("run".equals(command)) {
            cntr.start();
        }

        return cntr;
    }

    public static Engine createEngine(
            ServerConfig serverCfg,
            InterpreterFactory intFactory,
            Schema schm,
            Connector cntr,
            Collection cfgs,
            String command) throws Exception {

        EngineConfig engineCfg = serverCfg.getEngineConfig();
        Engine engine = new Engine();
        engine.setInterpreterFactory(intFactory);
        engine.setSchema(schm);
        engine.setConnector(cntr);
        engine.init(engineCfg);

        for (Iterator i=cfgs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            engine.addConfig(config);
        }

        return engine;
    }

    public void create() throws Exception {
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            create(config, null, entryDefinitions);
        }
    }

    public void create(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            if (entryDefinition.isDynamic()) continue;
            log.debug("Creating tables for "+entryDefinition.getDn());

            EngineCache cache = getCache(parentDn, entryDefinition);
            cache.create();

            Collection children = config.getChildren(entryDefinition);
            create(config, entryDefinition.getDn(), children);
        }
    }

    public void load() throws Exception {
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            load(config, null, entryDefinitions);
        }
    }

    public void load(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            if (entryDefinition.isDynamic()) continue;
            log.debug("Loading tables for "+entryDefinition.getDn());

            EngineCache cache = getCache(parentDn, entryDefinition);
            cache.load();

            Collection children = config.getChildren(entryDefinition);
            load(config, parentDn, children);
        }
    }

    public void clean() throws Exception {
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            clean(config, entryDefinitions);
        }
    }

    public void clean(Config config, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            if (entryDefinition.isDynamic()) continue;
            log.debug("Cleaning tables for "+entryDefinition.getDn());

            Collection children = config.getChildren(entryDefinition);
            clean(config, children);
        }
    }

    public void drop() throws Exception {
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            drop(config, entryDefinitions);
        }
    }

    public void drop(Config config, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            if (entryDefinition.isDynamic()) continue;
            log.debug("Dropping tables for "+entryDefinition.getDn());

            Collection children = config.getChildren(entryDefinition);
            drop(config, children);
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public InterpreterFactory getInterpreterFactory() {
        return interpreterFactory;
    }

    public void setInterpreterFactory(InterpreterFactory interpreterFactory) {
        this.interpreterFactory = interpreterFactory;
    }

    public TransformEngine getTransformEngine() {
        return transformEngine;
    }

    public void setTransformEngine(TransformEngine transformEngine) {
        this.transformEngine = transformEngine;
    }
}

