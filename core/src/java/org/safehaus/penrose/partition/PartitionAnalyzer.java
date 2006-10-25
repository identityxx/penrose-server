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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphEdge;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PartitionAnalyzer {

    Logger log = LoggerFactory.getLogger(getClass());

    private PartitionManager partitionManager;
    private InterpreterManager interpreterManager;

    public Map graphs = new HashMap();
    public Map primarySources = new HashMap();
    public Map uniqueness = new HashMap();

    public void analyze(Partition partition, EntryMapping entryMapping) throws Exception {

        log.debug("Analyzing entry "+entryMapping.getDn()+".");

        SourceMapping sourceMapping = computePrimarySource(entryMapping);
        if (sourceMapping != null) {
            primarySources.put(entryMapping, sourceMapping);
            //log.debug(" - primary sourceMapping: "+sourceMapping);
        }

        Graph graph = computeGraph(partition, entryMapping);

        if (graph != null) {
            graphs.put(entryMapping, graph);
            //log.debug(" - graph: "+graph);
        }

        boolean unique = isUnique(partition, entryMapping);
        log.debug("Unique: "+unique);

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();
            analyze(partition, childMapping);
        }
	}

    public Graph computeGraph(Partition partition, EntryMapping entryMapping) throws Exception {

        //log.debug("Graph for "+entryMapping":");

        //if (entryMappinges().isEmpty()) return null;

        Graph graph = new Graph();

        Collection sources = partition.getEffectiveSourceMappings(entryMapping);
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
            SourceMapping lsource = partition.getEffectiveSourceMapping(entryMapping, lsourceName);
            if (lsource == null) continue;

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            if (rindex < 0) continue;

            String rsourceName = rhs.substring(0, rindex);
            SourceMapping rsource = partition.getEffectiveSourceMapping(entryMapping, rsourceName);
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

    public SourceMapping computePrimarySource(EntryMapping entryMapping) throws Exception {

        Collection rdnAttributes = entryMapping.getRdnAttributeNames();
        if (rdnAttributes.isEmpty()) return null;

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

            Interpreter interpreter = interpreterManager.newInstance();

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

    public InterpreterManager getInterpreterManager() {
        return interpreterManager;
    }

    public void setInterpreterManager(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public Graph getGraph(EntryMapping entryMapping) throws Exception {
        return (Graph)graphs.get(entryMapping);
    }

    public SourceMapping getPrimarySource(EntryMapping entryMapping) throws Exception {
        return (SourceMapping)primarySources.get(entryMapping);
    }

    /**
     * Check whether each rdn value corresponds to one row from the source.
     */
    public boolean checkUniqueness(Partition partition, EntryMapping entryMapping) throws Exception {

        Collection rdnSources = new TreeSet();
        Collection rdnFields = new TreeSet();

        // check each RDN attribute
        Collection rdnAttributes = entryMapping.getRdnAttributeNames();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            //log.debug("Attribute "+attributeMapping.getName()+": "+attributeMapping.getType());

            if (attributeMapping.getVariable() != null) {
                String variable = attributeMapping.getVariable();

                int j = variable.indexOf(".");
                String sourceAlias = variable.substring(0, j);
                String fieldName = variable.substring(j+1);

                rdnSources.add(sourceAlias);
                rdnFields.add(fieldName);

                continue;
            }

            if (attributeMapping.getExpression() == null) continue;

            log.debug("RDN attribute "+attributeMapping.getName()+" is an expression.");
            return false;
        }

        //log.debug("RDN sources: "+rdnSources);

        if (rdnSources.isEmpty()) {
            log.debug("RDN attributes are constants.");
            return false;
        }

        if (rdnSources.size() > 1) {
            log.debug("RDN uses multiple sources: "+rdnSources);
            return false;
        }

        String sourceAlias = (String)rdnSources.iterator().next();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceAlias);
        if (sourceMapping == null) throw new Exception("Invalid source mapping \""+sourceAlias+"\" in \""+entryMapping.getDn()+"\".");

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());
        if (sourceMapping == null) throw new Exception("Invalid source reference \""+sourceMapping.getSourceName()+"\" in \""+entryMapping.getDn()+"\".");

        Collection uniqueFields = new TreeSet();
        Collection pkFields = new TreeSet();

        for (Iterator i=rdnFields.iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();
            if (fieldName.startsWith("primaryKey.")) continue;

            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
            if (fieldConfig == null) throw new Exception("Unknown field: "+fieldName);
            
            if (fieldConfig.isUnique()) {
                uniqueFields.add(fieldName);
                continue;
            }

            if (fieldConfig.isPK()) {
                pkFields.add(fieldName);
                continue;
            }

            log.debug("RDN uses non-unique field: "+fieldName);
            return false;
        }

        //log.debug("RDN unique fields: "+uniqueFields);
        //log.debug("RDN PK fields: "+pkFields);

        // rdn uses unique fields
        if (pkFields.isEmpty() && !uniqueFields.isEmpty()) return true;

        Collection list = sourceConfig.getPrimaryKeyNames();
        //log.debug("Source PK fields: "+list);

        if (!pkFields.equals(list)) {
            log.debug("RDN doesn't use all primary keys of "+sourceConfig.getName());
            return false;
        }

        return true;
    }

    public boolean isUnique(Partition partition, EntryMapping entryMapping) throws Exception {

        Boolean b = (Boolean)uniqueness.get(entryMapping);
        if (b != null) return b.booleanValue();

        b = new Boolean(checkUniqueness(partition, entryMapping));

        if (b.booleanValue()) { // check parent mapping
            EntryMapping parentMapping = partition.getParent(entryMapping);

            if (parentMapping != null) b = new Boolean(isUnique(partition, parentMapping));
        }

        uniqueness.put(entryMapping, b);
        return b.booleanValue();
    }
}
