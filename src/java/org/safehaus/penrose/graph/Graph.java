/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.graph;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Graph {

    private Set nodes = new HashSet();
    private Map edges = new HashMap();

    private Map indices = new HashMap();

    public void addNode(Object node) {
        nodes.add(node);
    }

    public Collection getNodes() {
        return nodes;
    }

    public void addEdge(Object node1, Object node2, Object object) throws Exception {
        if (!nodes.contains(node1)) throw new Exception("Node "+node1+" is not in the graph.");
        if (!nodes.contains(node2)) throw new Exception("Node "+node2+" is not in the graph.");

        Set edge = new HashSet();
        edge.add(node1);
        edge.add(node2);
        edges.put(edge, object);

        Set index1 = (Set)indices.get(node1);
        if (index1 == null) {
            index1 = new HashSet();
            indices.put(node1, index1);
        }
        index1.add(edge);

        Set index2 = (Set)indices.get(node2);
        if (index2 == null) {
            index2 = new HashSet();
            indices.put(node2, index2);
        }
        index2.add(edge);
    }

    public Collection getEdges() {
        return edges.keySet();
    }

    public Collection getEdges(Object node) throws Exception {
        if (!nodes.contains(node)) throw new Exception("Node "+node+" is not in the graph.");
        return (Collection)indices.get(node);
    }

    public Object getEdge(Object node1, Object node2) throws Exception {
        if (!nodes.contains(node1)) throw new Exception("Node "+node1+" is not in the graph.");
        if (!nodes.contains(node2)) throw new Exception("Node "+node2+" is not in the graph.");

        Set edge = new HashSet();
        edge.add(node1);
        edge.add(node2);
        return edges.get(edge);
    }

    public String toString() {
        return "Nodes: "+nodes+", Edges: "+edges.keySet();
    }

    public void traverse(GraphVisitor visitor) throws Exception {
        Object node = getNodes().iterator().next();
        traverse(visitor, node, null);
    }

    public void traverse(GraphVisitor visitor, Object node) throws Exception {
        traverse(visitor, node, null);
    }

    public void traverse(GraphVisitor visitor, Object node, Object parameter) throws Exception {
        new GraphIterator(this, visitor).traverse(node, parameter);
    }
}
