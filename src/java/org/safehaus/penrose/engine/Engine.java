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
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphEdge;
import org.safehaus.penrose.thread.ThreadPool;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Engine {

    static Logger log = Logger.getLogger(Engine.class);

    private PenroseConfig penroseConfig;
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
    private ConnectionManager connectionManager;

    private EngineFilterTool filterTool;

    private AddEngine addEngine;
    private DeleteEngine deleteEngine;
    private ModifyEngine modifyEngine;
    private ModRdnEngine modrdnEngine;

    private SearchEngine searchEngine;
    private LoadEngine loadEngine;
    private MergeEngine mergeEngine;
    private JoinEngine joinEngine;
    private TransformEngine transformEngine;

    private PartitionManager partitionManager;
    private Map caches = new TreeMap();

    /**
     * Initialize the engine with a Penrose instance
     *
     * @throws Exception
     */
    public void init(EngineConfig engineConfig) throws Exception {
        this.engineConfig = engineConfig;

        //log.debug("-------------------------------------------------");
        //log.debug("Initializing "+engineConfig.getEngineName()+" engine ...");

        filterTool = new EngineFilterTool(this);

        addEngine = new AddEngine(this);
        deleteEngine = new DeleteEngine(this);
        modifyEngine = new ModifyEngine(this);
        modrdnEngine = new ModRdnEngine(this);
        searchEngine = new SearchEngine(this);
        loadEngine = new LoadEngine(this);
        mergeEngine = new MergeEngine(this);
        joinEngine = new JoinEngine(this);
        transformEngine = new TransformEngine(this);
    }

    public void start() throws Exception {
        String s = engineConfig.getParameter(EngineConfig.THREAD_POOL_SIZE);
        int threadPoolSize = s == null ? EngineConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        threadPool = new ThreadPool(threadPoolSize);
        execute(new RefreshThread(this));
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                analyze(entryMapping);
            }
        }
    }

    public void analyze(EntryMapping entryMapping) throws Exception {

        //log.debug("Entry "+entryMapping":");

        SourceMapping sourceMapping = computePrimarySource(entryMapping);
        if (sourceMapping != null) {
            primarySources.put(entryMapping, sourceMapping);
            //log.debug(" - primary sourceMapping: "+sourceMapping);
        }

        Graph graph = computeGraph(entryMapping);
        if (graph != null) {
            graphs.put(entryMapping, graph);
            //log.debug(" - graph: "+graph);
        }

        Partition partition = partitionManager.getConfig(entryMapping);
        Collection children = partition.getChildren(entryMapping);
        if (children != null) {
            for (Iterator i=children.iterator(); i.hasNext(); ) {
                EntryMapping childMapping = (EntryMapping)i.next();
                analyze(childMapping);
            }
        }
	}

    public SourceMapping getPrimarySource(EntryMapping entryMapping) throws Exception {
        SourceMapping sourceMapping = (SourceMapping)primarySources.get(entryMapping);
        return sourceMapping;
/*
        if (source != null) return source;

        Config config = getConfig(entryDefinition.getDn());
        entryDefinition = config.getParent(entryDefinition);
        if (entryDefinition == null) return null;

        return getPrimarySource(entryDefinition);
*/
    }

    SourceMapping computePrimarySource(EntryMapping entryMapping) throws Exception {

        Collection rdnAttributes = entryMapping.getRdnAttributes();

        // TODO need to handle multiple rdn attributes
        AttributeMapping rdnAttribute = (AttributeMapping)rdnAttributes.iterator().next();

        if (rdnAttribute.getConstant() == null) {
            String variable = rdnAttribute.getVariable();
            if (variable != null) {
                int i = variable.indexOf(".");
                String sourceName = variable.substring(0, i);
                SourceMapping source = entryMapping.getSourceMapping(sourceName);
                return source;
            }

            Expression expression = rdnAttribute.getExpression();
            String foreach = expression.getForeach();
            if (foreach != null) {
                int i = foreach.indexOf(".");
                String sourceName = foreach.substring(0, i);
                SourceMapping source = entryMapping.getSourceMapping(sourceName);
                return source;
            }

            Interpreter interpreter = interpreterFactory.newInstance();

            Collection variables = interpreter.parseVariables(expression.getScript());

            for (Iterator i=variables.iterator(); i.hasNext(); ) {
                String sourceName = (String)i.next();
                SourceMapping source = entryMapping.getSourceMapping(sourceName);
                if (source != null) return source;
            }

            interpreter.clear();
        }

        Collection sources = entryMapping.getSourceMappings();
        if (sources.isEmpty()) return null;

        return (SourceMapping)sources.iterator().next();
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

    public Graph getGraph(EntryMapping entryMapping) throws Exception {

        return (Graph)graphs.get(entryMapping);
    }

    Graph computeGraph(EntryMapping entryMapping) throws Exception {

        //log.debug("Graph for "+entryMapping":");

        //if (entryMappinges().isEmpty()) return null;

        Graph graph = new Graph();

        Partition partition = partitionManager.getConfig(entryMapping);
        Collection sources = partition.getEffectiveSources(entryMapping);
        //if (sources.size() == 0) return null;

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping source = (SourceMapping)i.next();
            graph.addNode(source);
        }

        Collection relationships = partition.getEffectiveRelationships(entryMapping);
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            //log.debug("Checking ["+relationship.getExpression()+"]");

            String lhs = relationship.getLhs();
            int lindex = lhs.indexOf(".");
            if (lindex < 0) continue;

            String lsourceName = lhs.substring(0, lindex);
            SourceMapping lsource = partition.getEffectiveSource(entryMapping, lsourceName);
            if (lsource == null) continue;

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            if (rindex < 0) continue;

            String rsourceName = rhs.substring(0, rindex);
            SourceMapping rsource = partition.getEffectiveSource(entryMapping, rsourceName);
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

        //Collection edges = graph.getEdges();
        //for (Iterator i=edges.iterator(); i.hasNext(); ) {
            //GraphEdge edge = (GraphEdge)i.next();
            //log.debug(" - "+edge);
        //}

        return graph;
    }

    public void execute(Runnable runnable) throws Exception {
        if (threadPool == null) {
            runnable.run();
        } else {
            threadPool.execute(runnable);
        }
    }

    public void stop() throws Exception {
        if (stopping) return;
        stopping = true;

        // wait for all the worker threads to finish
        if (threadPool != null) threadPool.stopRequestAllWorkers();
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

        EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues attributeValues = entry.getAttributeValues();

        Collection set = attributeValues.get("userPassword");

        if (set != null) {
            for (Iterator i = set.iterator(); i.hasNext(); ) {
                String userPassword = (String)i.next();
                log.debug("userPassword: "+userPassword);
                if (PasswordUtil.comparePassword(password, userPassword)) return LDAPException.SUCCESS;
            }
        }

        Collection sources = entryMapping.getSourceMappings();
        Partition partition = partitionManager.getConfig(entryMapping);

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping source = (SourceMapping)i.next();

            ConnectionConfig connectionConfig = partition.getConnectionConfig(source.getConnectionName());
            SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

            Map entries = transformEngine.split(source, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                Row pk = (Row)j.next();
                AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+": "+sourceValues);

                int rc = connector.bind(sourceDefinition, entryMapping, sourceValues, password);
                if (rc == LDAPException.SUCCESS) return rc;
            }
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public int add(
            Entry parent,
            EntryMapping entryMapping,
            AttributeValues attributeValues)
            throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD", 80));
            log.debug(Formatter.displayLine("DN: "+entryMapping.getDn(), 80));

            log.debug(Formatter.displayLine("Attribute values:", 80));
            for (Iterator i = attributeValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection values = attributeValues.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return addEngine.add(parent, entryMapping, attributeValues);
    }

    public int delete(Entry entry) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displaySeparator(80));
        }

        return deleteEngine.delete(entry);
    }

    public int modrdn(Entry entry, String newRdn) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));
            log.debug(Formatter.displayLine("New RDN: "+newRdn, 80));
        }

        return modrdnEngine.modrdn(entry, newRdn);
    }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY", 80));
            log.debug(Formatter.displayLine("DN: "+entry.getDn(), 80));

            log.debug(Formatter.displayLine("Old attribute values:", 80));
            AttributeValues oldValues = entry.getAttributeValues();
            for (Iterator iterator = oldValues.getNames().iterator(); iterator.hasNext(); ) {
                String name = (String)iterator.next();
                Collection values = oldValues.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }

            log.debug(Formatter.displayLine("New attribute values:", 80));
            for (Iterator iterator = newValues.getNames().iterator(); iterator.hasNext(); ) {
                String name = (String)iterator.next();
                Collection values = newValues.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return modifyEngine.modify(entry, newValues);
    }

    public String getParentSourceValues(Collection path, EntryMapping entryMapping, AttributeValues parentSourceValues) throws Exception {
        Partition partition = partitionManager.getConfig(entryMapping);
        EntryMapping parentMapping = partition.getParent(entryMapping);

        String prefix = null;
        Interpreter interpreter = interpreterFactory.newInstance();

        //log.debug("Parents' source values:");
        for (Iterator iterator = path.iterator(); iterator.hasNext(); ) {
            Map map = (Map)iterator.next();
            //String dn = (String)map.get("dn");
            Entry entry = (Entry)map.get("entry");
            //log.debug(" - "+dn);

            prefix = prefix == null ? "parent" : "parent."+prefix;

            if (entry == null) {
                AttributeValues av = computeAttributeValues(parentMapping, interpreter);
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

            parentMapping = partition.getParent(parentMapping);
        }

        return prefix;
    }

    /**
     * Check whether the entry uses no sources and all attributes are constants.
     */
    public boolean isStatic(EntryMapping entryMapping) throws Exception {
        Partition partition = partitionManager.getConfig(entryMapping);
        Collection effectiveSources = partition.getEffectiveSources(entryMapping);
        if (effectiveSources.size() > 0) return false;

        Collection attributeDefinitions = entryMapping.getAttributeMappings();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (attributeMapping.getConstant() == null) return false;
        }

        EntryMapping parentMapping = partition.getParent(entryMapping);
        if (parentMapping == null) return true;

        return isStatic(parentMapping);
    }

    /**
     * Check whether each rdn value corresponds to one row from the source.
     */
    public boolean isUnique(EntryMapping entryMapping) throws Exception {

        Collection rdnSources = new TreeSet();
        Collection rdnFields = new TreeSet();

        Collection rdnAttributes = entryMapping.getRdnAttributes();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            if (attributeMapping.getConstant() != null) continue;

            String variable = attributeMapping.getVariable();

            if (variable != null) { // get the sourceMapping and field name
                int j = variable.indexOf(".");
                String sourceAlias = variable.substring(0, j);
                String fieldName = variable.substring(j+1);

                rdnSources.add(sourceAlias);
                rdnFields.add(fieldName);

                continue;
            }

            // attribute is an expression
            return false;
        }

        //log.debug("RDN sources: "+rdnSources);

        // rdn is constant
        if (rdnSources.isEmpty()) return false;

        // rdn uses more than one sourceMapping
        if (rdnSources.size() > 1) return false;

        String sourceAlias = (String)rdnSources.iterator().next();
        SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceAlias);

        Partition partition = partitionManager.getConfig(entryMapping);
        ConnectionConfig connectionConfig = partition.getConnectionConfig(sourceMapping.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(sourceMapping.getSourceName());

        Collection uniqueFields = new TreeSet();
        Collection pkFields = new TreeSet();

        for (Iterator i=rdnFields.iterator(); i.hasNext(); ) {
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

        EntryMapping parentMapping = partition.getParent(entryMapping);
        if (parentMapping == null) return result;

        return isUnique(parentMapping);
    }

    public Entry find(
            Collection path,
            EntryMapping entryMapping
            ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Parents:", 80));

            for (Iterator i=path.iterator(); i.hasNext(); ) {
                Map map = (Map)i.next();
                String dn = (String)map.get("dn");
                log.debug(Formatter.displayLine(" - "+dn, 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        AttributeValues parentSourceValues = new AttributeValues();

        SearchResults results = search(path, parentSourceValues, entryMapping, null, null);

        if (results.size() == 0) return null;

        Entry entry = (Entry)results.next();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));
            log.debug(Formatter.displayLine("dn: "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        return entry;
    }

    public SearchResults search(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            Collection attributeNames) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));
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
        }

        final SearchResults entries = new SearchResults();
        final SearchResults results = new SearchResults();

        searchEngine.search(path, parentSourceValues, entryMapping, filter, entries);

        Collection attributeDefinitions = entryMapping.getAttributeMappings(attributeNames);
        //log.debug("Attribute definitions: "+attributeDefinitions);

        // check if client only requests the dn to be returned
        final boolean dnOnly = attributeNames != null && attributeNames.contains("dn")
                && attributeDefinitions.isEmpty()
                && "(objectclass=*)".equals(filter.toString().toLowerCase());

        if (dnOnly) {
            Interpreter interpreter = getInterpreterFactory().newInstance();

           for (Iterator i=entries.iterator(); i.hasNext(); ) {
               Map map = (Map)i.next();
               String dn = (String)map.get("dn");
               AttributeValues sv = (AttributeValues)map.get("sourceValues");

               AttributeValues attributeValues = computeAttributeValues(entryMapping, sv, interpreter);
               Entry entry = new Entry(dn, entryMapping, sv, attributeValues);

               results.add(entry);
           }
           return results;
        }

        SearchResults loadedEntries = new SearchResults();
        loadEngine.load(entryMapping, entries, loadedEntries, results);

        mergeEngine.merge(entryMapping, loadedEntries, results);

        return results;
    }

    public String getStartingSourceName(EntryMapping entryMapping) throws Exception {

        log.debug("Searching the starting sourceMapping for "+entryMapping.getDn());

        Partition partition = partitionManager.getConfig(entryMapping);

        Collection relationships = entryMapping.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            for (Iterator j=relationship.getOperands().iterator(); j.hasNext(); ) {
                String operand = j.next().toString();

                int index = operand.indexOf(".");
                if (index < 0) continue;

                String sourceName = operand.substring(0, index);
                SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
                SourceMapping effectiveSourceMapping = partition.getEffectiveSource(entryMapping, sourceName);

                if (sourceMapping == null && effectiveSourceMapping != null) {
                    log.debug("Source "+sourceName+" is defined in parent entry");
                    return sourceName;
                }

            }
        }

        Iterator i = entryMapping.getSourceMappings().iterator();
        if (!i.hasNext()) return null;

        SourceMapping sourceMapping = (SourceMapping)i.next();
        log.debug("Source "+sourceMapping.getName()+" is the first defined in entry");
        return sourceMapping.getName();
    }

    public Relationship getConnectingRelationship(EntryMapping entryMapping) throws Exception {

        // log.debug("Searching the connecting relationship for "+entryMapping;

        Partition partition = partitionManager.getConfig(entryMapping);

        Collection relationships = partition.getEffectiveRelationships(entryMapping);

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            // log.debug(" - checking "+relationship);

            String lhs = relationship.getLhs();
            String rhs = relationship.getRhs();

            int lindex = lhs.indexOf(".");
            String lsourceName = lindex < 0 ? lhs : lhs.substring(0, lindex);
            SourceMapping lsource = entryMapping.getSourceMapping(lsourceName);

            int rindex = rhs.indexOf(".");
            String rsourceName = rindex < 0 ? rhs : rhs.substring(0, rindex);
            SourceMapping rsource = entryMapping.getSourceMapping(rsourceName);

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

    public Filter createFilter(SourceMapping sourceMapping, Collection pks) throws Exception {

        Collection normalizedFilters = null;
        if (pks != null) {
            normalizedFilters = new TreeSet();
            for (Iterator i=pks.iterator(); i.hasNext(); ) {
                Row filter = (Row)i.next();

                Row f = new Row();
                for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    if (!name.startsWith(sourceMapping.getName()+".")) continue;

                    String newName = name.substring(sourceMapping.getName().length()+1);
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
            SourceMapping sourceMapping,
            EntryMapping entryMapping,
            Row rdn) throws Exception {

        if (sourceMapping == null) {
            return new Row();
        }

        Partition partition = partitionManager.getConfig(entryMapping);
        Collection fields = partition.getSearchableFields(sourceMapping);

        interpreter.set(rdn);

        Row filter = new Row();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)j.next();
            String name = fieldMapping.getName();

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            //log.debug("   ==> "+field.getName()+"="+value);
            //filter.set(source.getName()+"."+name, value);
            filter.set(name, value);
        }

        //if (filter.isEmpty()) return null;

        interpreter.clear();

        return filter;
    }

    public Filter generateFilter(SourceMapping sourceMapping, Collection relationships, Collection rows) throws Exception {
        log.debug("Generating filters for source "+sourceMapping.getName());

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

                if (rhs.startsWith(sourceMapping.getName()+".")) {
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

    public Filter generateFilter(SourceMapping toSource, Collection relationships, AttributeValues av) throws Exception {
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
            //String lsourceName = lhs.substring(0, lindex);
            String lname = lhs.substring(lindex+1);

            //int rindex = rhs.indexOf(".");
            //String rsourceName = rhs.substring(0, rindex);
            //String rname = rhs.substring(rindex+1);

            //log.debug("   converting "+rhs+" ==> ("+lname+" "+operator+" ?)");

            Collection v = av.get(rhs);
            //log.debug("   - found "+v);
            if (v == null) continue;

            Filter orFilter = null;
            for (Iterator k=v.iterator(); k.hasNext(); ) {
                Object value = k.next();

                SimpleFilter sf = new SimpleFilter(lname, operator, value.toString());
                //log.debug("   - "+sf);

                orFilter = FilterTool.appendOrFilter(orFilter, sf);
            }
            log.debug("   - "+orFilter);

            filter = FilterTool.appendAndFilter(filter, orFilter);
        }

        return filter;
    }

    public Collection computeDns(
            Interpreter interpreter,
            EntryMapping entryMapping,
            AttributeValues sourceValues)
            throws Exception {

        interpreter.set(sourceValues);

        Collection results = new ArrayList();

        results.addAll(computeDns(interpreter, entryMapping));

        interpreter.clear();

        return results;
    }

    public Collection computeDns(Interpreter interpreter, EntryMapping entryMapping) throws Exception {

        Partition partition = partitionManager.getConfig(entryMapping);
        EntryMapping parentMapping = partition.getParent(entryMapping);

        Collection parentDns;
        if (parentMapping != null) {
            parentDns = computeDns(interpreter, parentMapping);
        } else {
            parentDns = new ArrayList();
            parentDns.add(entryMapping.getParentDn());
        }

        Collection rdns = computeRdn(interpreter, entryMapping);
        Collection dns = new ArrayList();

        for (Iterator i=parentDns.iterator(); i.hasNext(); ) {
            String parentDn = (String)i.next();
            for (Iterator j=rdns.iterator(); j.hasNext(); ) {
                Row rdn = (Row)j.next();
                String dn = rdn +(parentDn == null ? "" : ","+parentDn);
                dns.add(dn);
            }
        }

        return dns;
    }

    public Collection computeRdn(
            Interpreter interpreter,
            EntryMapping entryMapping
            ) throws Exception {

        AttributeValues rdns = new AttributeValues();

        Collection rdnAttributes = entryMapping.getRdnAttributes();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            rdns.add(name, value);
        }

        return TransformEngine.convert(rdns);
    }

    public AttributeValues computeAttributeValues(
            EntryMapping entryMapping,
            Interpreter interpreter
            ) throws Exception {

        return computeAttributeValues(entryMapping, null, interpreter);
    }

    public AttributeValues computeAttributeValues(
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            Interpreter interpreter
            ) throws Exception {

        AttributeValues attributeValues = new AttributeValues();

        if (sourceValues != null) interpreter.set(sourceValues);

        Collection attributeDefinitions = entryMapping.getAttributeMappings();
        for (Iterator j=attributeDefinitions.iterator(); j.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)j.next();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            String name = attributeMapping.getName();
            attributeValues.add(name, value);
        }

        interpreter.clear();

        Collection objectClasses = entryMapping.getObjectClasses();
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

    public EntryCache getCache(String parentDn, EntryMapping entryMapping) throws Exception {
        String cacheName = entryMapping.getParameter(EntryMapping.CACHE);
        cacheName = cacheName == null ? EntryMapping.DEFAULT_CACHE : cacheName;
        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();

        String key = entryMapping.getRdn()+","+parentDn;

        EntryCache cache = (EntryCache)caches.get(key);

        if (cache == null) {

            String cacheClass = cacheConfig.getCacheClass();
            cacheClass = cacheClass == null ? EngineConfig.DEFAULT_CACHE_CLASS : cacheClass;

            Class clazz = Class.forName(cacheClass);
            cache = (EntryCache)clazz.newInstance();

            cache.setParentDn(parentDn);
            cache.setEntryMapping(entryMapping);
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

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public void setServerConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }
}

