/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
import org.safehaus.penrose.connector.ConnectorManager;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.entry.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.Attributes;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class Engine {

    public Logger log = LoggerFactory.getLogger(getClass());

    public PenroseConfig penroseConfig;
    public PenroseContext penroseContext;

    public EngineConfig engineConfig;

    public SchemaManager schemaManager;
    public InterpreterManager interpreterManager;
    public ConnectorManager connectorManager;
    public ConnectionManager connectionManager;
    public PartitionManager partitionManager;

    public boolean stopping = false;

    public EngineFilterTool engineFilterTool;
    private FilterTool      filterTool;

    public TransformEngine transformEngine;

    protected Analyzer analyzer;

    public void init() throws Exception {
        filterTool = new FilterTool();
        filterTool.setSchemaManager(schemaManager);

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

    public void setInterpreterManager(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public Connector getConnector(SourceConfig sourceConfig) {
        String connectorName = sourceConfig.getParameter("connectorName");
        return connectorManager.getConnector(connectorName);
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

    public SourceMapping getPrimarySource(EntryMapping entryMapping) throws Exception {
        return analyzer.getPrimarySource(entryMapping);
    }

    /**
     * Compute attribute values of an entry given the source values
     * @param entryMapping
     * @param sourceValues
     * @param interpreter
     * @return attribute values
     * @throws Exception
     */
    public AttributeValues computeAttributeValues(
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            Interpreter interpreter
            ) throws Exception {

        AttributeValues attributeValues = new AttributeValues();
        if (sourceValues != null) interpreter.set(sourceValues);

        Collection attributeMappings = entryMapping.getAttributeMappings();
        //log.debug("Attributes:");

        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            String name = attributeMapping.getName();
            Object value = interpreter.eval(entryMapping, attributeMapping);

            if (value == null) {
                if (attributeMapping.isRdn()) {
                    //log.debug("Primary key "+name+" is null.");
                    return null;
                }

                //log.debug(" - "+name+": null");
                continue;
            }

            attributeValues.add(name, value);

            //String className = value.getClass().getName();
            //className = className.substring(className.lastIndexOf(".")+1);
            //log.debug(" - "+name+": "+value+" ("+className+")");
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
    }

    public void stop() throws Exception {

    }

    public String getStartingSourceName(Partition partition, EntryMapping entryMapping) throws Exception {

        log.debug("Searching the starting sourceMapping for "+entryMapping.getDn());

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
            RDN rdn = (RDN)i.next();
            log.debug(" - "+rdn);

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
                Object value = rdn.get(rhs);
                if (value == null) continue;

                SimpleFilter sf = new SimpleFilter(name, operator, value.toString());
                log.debug("     --> "+sf);

                subFilter = FilterTool.appendAndFilter(subFilter, sf);
            }

            filter = FilterTool.appendOrFilter(filter, subFilter);
        }

        return filter;
    }

    public Filter generateFilter(SourceMapping toSource, Collection relationships, AttributeValues sv) throws Exception {
/*
        log.debug("Generating filters using source values:");
        for (Iterator i=sv.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sv.get(name);
            log.debug(" - "+name+": "+values);
        }
*/
        Filter filter = null;

        for (Iterator j=relationships.iterator(); j.hasNext(); ) {
            Relationship relationship = (Relationship)j.next();
            //log.debug("Relationship "+relationship);

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

            Collection v = sv.get(rhs);
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

    public boolean isStopping() {
        return stopping;
    }

    /**
     * Check whether the entry uses no sources and all attributes are constants.
     */
    public boolean isStatic(Partition partition, EntryMapping entryMapping) throws Exception {
        Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);
        if (effectiveSources.size() > 0) return false;

        Collection attributeDefinitions = entryMapping.getAttributeMappings();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            if (attributeMapping.getConstant() == null) return false;
        }

        EntryMapping parentMapping = partition.getParent(entryMapping);
        if (parentMapping == null) return true;

        return isStatic(partition, parentMapping);
    }

    public EngineFilterTool getEngineFilterTool() {
        return engineFilterTool;
    }

    public void setEngineFilterTool(EngineFilterTool engineFilterTool) {
        this.engineFilterTool = engineFilterTool;
    }

    public boolean isUnique(Partition partition, EntryMapping entryMapping) throws Exception {
        return analyzer.isUnique(partition, entryMapping);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void bind(
            PenroseSession session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String password
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void add(
            PenroseSession session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            DN dn,
            Attributes attributes
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void delete(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void modify(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            Collection modifications
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public abstract void modrdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Entry find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {
        return null;
    }

    public void search(
            PenroseSession session,
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            DN baseDn,
            Filter filter,
            PenroseSearchControls sc,
            Results results
    ) throws Exception {

        search(
                session,
                partition,
                sourceValues,
                entryMapping,
                entryMapping,
                baseDn,
                filter,
                sc,
                results
        );
    }

    public abstract void search(
            PenroseSession session,
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping baseMapping,
            EntryMapping entryMapping,
            DN baseDn,
            Filter filter,
            PenroseSearchControls sc,
            Results results
    ) throws Exception;

    public Relationship getConnectingRelationship(Partition partition, EntryMapping entryMapping) throws Exception {

        // log.debug("Searching the connecting relationship for "+entryMapping;

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

        String prefix = sourceMapping.getName()+".";
        int length = prefix.length();

        if (pks == null) return null;

        Collection normalizedFilters = new TreeSet();
        RDNBuilder rb = new RDNBuilder();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            RDN filter = (RDN)i.next();

            rb.clear();
            for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                if (!name.startsWith(prefix)) continue;

                String newName = name.substring(length);
                rb.set(newName, filter.get(name));
            }

            if (rb.isEmpty()) continue;

            rb.normalize();
            RDN normalizedFilter = rb.toRdn();
            normalizedFilters.add(normalizedFilter);
        }

        return FilterTool.createFilter(normalizedFilters);
    }

    public RDN createFilter(
            Partition partition,
            Interpreter interpreter,
            SourceMapping sourceMapping,
            EntryMapping entryMapping,
            RDN rdn) throws Exception {

        if (sourceMapping == null) {
            return new RDN();
        }

        Collection fields = partition.getSearchableFields(sourceMapping);

        interpreter.set(rdn);

        RDNBuilder rb = new RDNBuilder();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)j.next();
            String name = fieldMapping.getName();

            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            //log.debug("   ==> "+field.getName()+"="+value);
            //rb.set(source.getName()+"."+name, value);
            rb.set(name, value);
        }

        //if (rb.isEmpty()) return null;

        interpreter.clear();

        return rb.toRdn();
    }

    public Collection computeDns(
            Partition partition,
            Interpreter interpreter,
            EntryMapping entryMapping,
            AttributeValues sourceValues)
            throws Exception {

        interpreter.set(sourceValues);

        log.debug("Generating DNs:");
        Collection dns = new ArrayList();
        computeDns(partition, interpreter, entryMapping, dns);

        interpreter.clear();

        return dns;
    }

    public void computeDns(Partition partition, Interpreter interpreter, EntryMapping entryMapping, Collection dns) throws Exception {

        EntryMapping parentMapping = partition.getParent(entryMapping);

        Collection parentDns = new ArrayList();
        if (parentMapping != null) {
            computeDns(partition, interpreter, parentMapping, parentDns);

        } else if (!entryMapping.getParentDn().isEmpty()) {
            parentDns.add(entryMapping.getParentDn());
        }

        if (parentDns.isEmpty()) {
            DN dn = entryMapping.getDn();
            log.debug(" - "+dn);
            dns.add(dn);

        } else {
            Collection rdns = computeRdns(interpreter, entryMapping);

            DNBuilder db = new DNBuilder();

            for (Iterator iterator=rdns.iterator(); iterator.hasNext(); ) {
                RDN rdn = (RDN)iterator.next();
                //log.info("Processing RDN: "+rdn);

                for (Iterator j=parentDns.iterator(); j.hasNext(); ) {
                    DN parentDn = (DN)j.next();
                    //log.debug("Appending parent DN: "+parentDn);

                    db.set(rdn);
                    db.append(parentDn);
                    DN dn = db.toDn();

                    log.debug(" - "+dn);
                    dns.add(dn);
                }
            }
        }
    }

    public Collection computeRdns(
            Interpreter interpreter,
            EntryMapping entryMapping
            ) throws Exception {

        //log.debug("Computing RDNs:");
        AttributeValues rdns = new AttributeValues();

        Collection rdnAttributes = entryMapping.getRdnAttributeMappings();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            String name = attributeMapping.getName();

            Object value = interpreter.eval(entryMapping, attributeMapping);
            if (value == null) continue;

            rdns.add(name, value);
        }

        return TransformEngine.convert(rdns);
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }

    public ConnectorManager getConnectorManager() {
        return connectorManager;
    }

    public void setConnectorManager(ConnectorManager connectorManager) {
        this.connectorManager = connectorManager;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;

        schemaManager = penroseContext.getSchemaManager();
        interpreterManager = penroseContext.getInterpreterManager();
        connectorManager = penroseContext.getConnectorManager();
        connectionManager =penroseContext.getConnectionManager();
        partitionManager = penroseContext.getPartitionManager();
    }
}

