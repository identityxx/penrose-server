package org.safehaus.penrose.engine;

import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphEdge;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Analyzer {

    Logger log = LoggerFactory.getLogger(getClass());

    private PartitionManager partitionManager;
    private InterpreterManager interpreterManager;

    public Map graphs = new HashMap();
    public Map primarySources = new HashMap();

    public void analyze(EntryMapping entryMapping) throws Exception {

        log.debug("Analyzing entry "+entryMapping.getDn()+".");

        SourceMapping sourceMapping = computePrimarySource(entryMapping);
        if (sourceMapping != null) {
            primarySources.put(entryMapping, sourceMapping);
            //log.debug(" - primary sourceMapping: "+sourceMapping);
        }

        Partition partition = partitionManager.getPartition(entryMapping);
        Graph graph = computeGraph(partition, entryMapping);

        if (graph != null) {
            graphs.put(entryMapping, graph);
            //log.debug(" - graph: "+graph);
        }

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();
            analyze(childMapping);
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

        Collection rdnAttributes = entryMapping.getRdnAttributes();
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

}
