/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.graph;

import org.safehaus.penrose.graph.Graph;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

/**
 * @author Endi S. Dewata
 */
public class GraphIterator {

    public Graph graph;
    public GraphVisitor visitor;

    public GraphIterator(Graph graph, GraphVisitor visitor) {
        this.graph = graph;
        this.visitor = visitor;
    }

    public void traverse(Object node, Object parameter) throws Exception {
        Set visitedNodes = new HashSet();
        Set visitedEdges = new HashSet();
        traverse(node, parameter, visitedNodes, visitedEdges);
    }

    public void traverse(Object node, Object parameter, Set visitedNodes, Set visitedEdges) throws Exception {
        if (visitedNodes.contains(node)) return;

        visitedNodes.add(node);
        boolean b = visitor.preVisitNode(node, parameter);
        if (!b) return;

        Collection edges = graph.getEdges(node);
        if (edges == null) return;

        for (Iterator i=edges.iterator(); i.hasNext(); ) {
            Set edge = (Set)i.next();

            if (visitedEdges.contains(edge)) continue;

            visitedEdges.add(edge);

            Iterator j=edge.iterator();
            Object n1 = j.next();
            Object n2 = j.next();

            if (node == n2) { // move from left to right
                Object n = n1;
                n1 = n2;
                n2 = n;
            }

            Object object = graph.getEdge(n1, n2);
            b = visitor.preVisitEdge(n1, n2, object, parameter);
            if (!b) continue;

            traverse(n2, parameter, visitedNodes, visitedEdges);

            visitor.postVisitEdge(n1, n2, object, parameter);
        }

        visitor.postVisitNode(node, parameter);
    }
}
