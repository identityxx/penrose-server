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

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.thread.Queue;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.util.EntryUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.Attributes;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class Engine {

    public static Logger log = LoggerFactory.getLogger(Engine.class);

    public EngineConfig engineConfig;
    public PenroseConfig penroseConfig;
    public SchemaManager schemaManager;
    public InterpreterManager interpreterManager;
    public Connector connector;
    public ConnectionManager connectionManager;
    public PartitionManager partitionManager;
    public EntryCache entryCache;

    public boolean stopping = false;

    ThreadManager threadManager;
    public EngineFilterTool engineFilterTool;
    private FilterTool filterTool;

    public Map locks = new HashMap();
    public Queue queue = new Queue();

    public LoadEngine loadEngine;
    public JoinEngine joinEngine;
    public MergeEngine mergeEngine;

    public TransformEngine transformEngine;

    Analyzer analyzer;

    public void init() throws Exception {
        analyzer = new Analyzer();
        analyzer.setPartitionManager(partitionManager);
        analyzer.setInterpreterManager(interpreterManager);
    }

    public EngineConfig getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public InterpreterManager getInterpreterManager() {
        return interpreterManager;
    }

    public void setInterpreterFactory(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;
    }

    public EntryCache getEntryCache() {
        return entryCache;
    }

    public void setEntryCache(EntryCache entryCache) {
        this.entryCache = entryCache;
    }

    public SourceMapping getPrimarySource(EntryMapping entryMapping) throws Exception {
        return analyzer.getPrimarySource(entryMapping);
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

        return computeAttributeValues(entryMapping, sourceValues, null, interpreter);
    }

    /**
     * Compute attribute values of an entry given the source values or row values
     * @param entryMapping
     * @param sourceValues
     * @param rows
     * @param interpreter
     * @return attribute values
     * @throws Exception
     */
    public AttributeValues computeAttributeValues(
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            Collection rows,
            Interpreter interpreter
            ) throws Exception {

        AttributeValues attributeValues = new AttributeValues();

        if (sourceValues != null) interpreter.set(sourceValues, rows);

        //log.debug("Attribute values:");
        Collection attributeMappings = entryMapping.getAttributeMappings();
        for (Iterator j=attributeMappings.iterator(); j.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)j.next();

            String name = attributeMapping.getName();
            //log.debug(" - "+name+":");

            Object value = interpreter.eval(entryMapping, attributeMapping);
            if (value == null) continue;

            attributeValues.add(name, value);
            //log.debug("   - "+value+" ("+value.getClass().getName()+")");
        }

        interpreter.clear();

        Collection objectClasses = entryMapping.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributeValues.add("objectClass", objectClass);
        }

        return attributeValues;
    }

    public TransformEngine getTransformEngine() {
        return transformEngine;
    }

    public void setTransformEngine(TransformEngine transformEngine) {
        this.transformEngine = transformEngine;
    }

    public Graph getGraph(EntryMapping entryMapping) throws Exception {
        return analyzer.getGraph(entryMapping);
    }

    public void start() throws Exception {
        filterTool = new FilterTool();
        filterTool.setSchemaManager(schemaManager);

    }

    public void stop() throws Exception {

    }

    public String getStartingSourceName(EntryMapping entryMapping) throws Exception {

        log.debug("Searching the starting sourceMapping for "+entryMapping.getDn());

        Partition partition = partitionManager.getPartition(entryMapping);

        Collection relationships = entryMapping.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            for (Iterator j=relationship.getOperands().iterator(); j.hasNext(); ) {
                String operand = j.next().toString();

                int index = operand.indexOf(".");
                if (index < 0) continue;

                String sourceName = operand.substring(0, index);
                SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
                SourceMapping effectiveSourceMapping = partition.getEffectiveSourceMapping(entryMapping, sourceName);

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

    public MergeEngine getMergeEngine() {
        return mergeEngine;
    }

    public void setMergeEngine(MergeEngine mergeEngine) {
        this.mergeEngine = mergeEngine;
    }

    public JoinEngine getJoinEngine() {
        return joinEngine;
    }

    public void setJoinEngine(JoinEngine joinEngine) {
        this.joinEngine = joinEngine;
    }

    public boolean isStopping() {
        return stopping;
    }

    /**
     * Check whether the entry uses no sources and all attributes are constants.
     */
    public boolean isStatic(EntryMapping entryMapping) throws Exception {
        Partition partition = partitionManager.getPartition(entryMapping);
        Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);
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

    public EngineFilterTool getEngineFilterTool() {
        return engineFilterTool;
    }

    public void setEngineFilterTool(EngineFilterTool engineFilterTool) {
        this.engineFilterTool = engineFilterTool;
    }

    public AttributeValues shiftParentSourceValues(AttributeValues attributeValues) {
        AttributeValues newSourceValues = new AttributeValues();

        for (Iterator j=attributeValues.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Collection values = attributeValues.get(name);

            if (name.startsWith("parent.")) name = "parent."+name;
            newSourceValues.add(name, values);
        }

        return newSourceValues;
    }

    public AttributeValues getParentSourceValues(
            Partition partition,
            Collection path
    ) throws Exception {

        AttributeValues sourceValues = new AttributeValues();

        String prefix = null;
        Interpreter interpreter = interpreterManager.newInstance();

        //log.debug("Parents' source values:");
        for (Iterator iterator = path.iterator(); iterator.hasNext(); ) {
            Entry entry = (Entry)iterator.next();
            EntryMapping parentMapping = entry.getEntryMapping();

            //log.debug(" - "+dn);

            prefix = prefix == null ? "parent" : "parent."+prefix;

            if (entry == null) {
                AttributeValues av = computeAttributeValues(parentMapping, interpreter);
                for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = av.get(name);
                    name = prefix+"."+name;
                    //log.debug("   - "+name+": "+values);
                    sourceValues.add(name, values);
                }
                interpreter.clear();

            } else {
                AttributeValues av = entry.getAttributeValues();
                for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = av.get(name);
                    name = prefix+"."+name;
                    //log.debug("   - "+name+": "+values);
                    sourceValues.add(name, values);
                }

                AttributeValues sv = entry.getSourceValues();
                for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = sv.get(name);
                    if (name.startsWith("parent.")) name = prefix+"."+name;
                    //log.debug("   - "+name+": "+values);
                    sourceValues.add(name, values);
                }
            }

            parentMapping = partition.getParent(parentMapping);
        }

        return sourceValues;
    }

    public void load(
            EntryMapping entryMapping,
            PenroseSearchResults entriesToLoad,
            PenroseSearchResults loadedEntries)
            throws Exception {

        loadEngine.load(entryMapping, entriesToLoad, loadedEntries);
    }

    public boolean isUnique(EntryMapping entryMapping) throws Exception {
        return analyzer.isUnique(entryMapping);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract int bind(
            Partition partition,
            Entry entry,
            String password
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract int add(
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            String dn,
            Attributes attributes
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract int delete(
            Partition partition,
            Entry entry
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract int modify(
            Partition partition,
            Entry entry,
            Collection modifications
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract int modrdn(
            Partition partition,
            Entry entry,
            String newRdn
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract int search(
            Partition partition,
            Collection path,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws Exception;

    public abstract int expand(
            Partition partition,
            Collection path,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            PenroseSearchResults results
    ) throws Exception;

    public synchronized MRSWLock getLock(String dn) {

		MRSWLock lock = (MRSWLock)locks.get(dn);

		if (lock == null) lock = new MRSWLock(queue);
		locks.put(dn, lock);

		return lock;
	}

    public void merge(
            EntryMapping entryMapping,
            PenroseSearchResults loadedEntries,
            PenroseSearchResults newEntries)
            throws Exception {

        mergeEngine.merge(entryMapping, loadedEntries, newEntries);
    }

    public Relationship getConnectingRelationship(EntryMapping entryMapping) throws Exception {

        // log.debug("Searching the connecting relationship for "+entryMapping;

        Partition partition = partitionManager.getPartition(entryMapping);

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

                Row normalizedFilter = schemaManager.normalize(f);
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

        Partition partition = partitionManager.getPartition(entryMapping);
        Collection fields = partition.getSearchableFields(sourceMapping);

        interpreter.set(rdn);

        Row filter = new Row();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)j.next();
            String name = fieldMapping.getName();

            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            //log.debug("   ==> "+field.getName()+"="+value);
            //filter.set(source.getName()+"."+name, value);
            filter.set(name, value);
        }

        //if (filter.isEmpty()) return null;

        interpreter.clear();

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

        Partition partition = partitionManager.getPartition(entryMapping);
        EntryMapping parentMapping = partition.getParent(entryMapping);

        Collection parentDns;
        if (parentMapping != null) {
            parentDns = computeDns(interpreter, parentMapping);
        } else {
            parentDns = new ArrayList();
            if (entryMapping.getParentDn() != null) {
                parentDns.add(entryMapping.getParentDn());
            }
        }

        Collection rdns = computeRdn(interpreter, entryMapping);
        Collection dns = new ArrayList();

        //log.info("Computing DNs for \""+entryMapping.getDn()+"\"");

        if (parentDns.isEmpty()) {
            dns.add(entryMapping.getDn());
        } else {
            for (Iterator i=parentDns.iterator(); i.hasNext(); ) {
                String parentDn = (String)i.next();
                //log.info(" - parent dn: "+parentDn);
                for (Iterator j=rdns.iterator(); j.hasNext(); ) {
                    Row rdn = (Row)j.next();
                    //log.info("   - rdn: "+rdn);

                    String s = rdn.toString(); //.trim();
                    //s = LDAPDN.escapeRDN(s);
                    //log.info("     => rdn: "+rdn);

                    String dn = EntryUtil.append(s, parentDn);

                    //log.info("     => dn: "+dn);
                    dns.add(dn);
                }
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

            Object value = interpreter.eval(entryMapping, attributeMapping);
            if (value == null) continue;

            rdns.add(name, value);
        }

        return TransformEngine.convert(rdns);
    }

    public LoadEngine getLoadEngine() {
        return loadEngine;
    }

    public void setLoadEngine(LoadEngine loadEngine) {
        this.loadEngine = loadEngine;
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }
}

