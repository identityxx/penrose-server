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
package org.safehaus.penrose.graph;

import org.safehaus.penrose.graph.Graph;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class GraphIterator {

    public Graph graph;
    public GraphVisitor visitor;

    Set visitedNodes = new HashSet();
    Set visitedEdges = new HashSet();

    public GraphIterator(Graph graph, GraphVisitor visitor) {
        this.graph = graph;
        this.visitor = visitor;
    }

    public void traverse(Object node) throws Exception {
        if (visitedNodes.contains(node)) return;

        visitedNodes.add(node);
        boolean b = visitor.preVisitNode(node);
        if (!b) return;

        visitor.visitNode(this, node);

        visitor.postVisitNode(node);
    }

    public void traverseEdges(Object node) throws Exception {
        Collection edges = graph.getEdges(node);
        if (edges == null) return;

        for (Iterator i=edges.iterator(); i.hasNext(); ) {
            Set edge = (Set)i.next();

            if (visitedEdges.contains(edge)) continue;

            visitedEdges.add(edge);
            Object object = graph.getEdgeObject(edge);

            Collection nodes = new ArrayList();
            Object node1 = null;
            Object node2 = null;

            if (edge.size() == 1) {
                node1 = edge.iterator().next();
                nodes.addAll(edge);

            } else {
                Iterator j=edge.iterator();
                node1 = j.next();
                node2 = j.next();

                if (node == node2) { // move from left to right
                    Object n = node1;
                    node1 = node2;
                    node2 = n;
                }

                nodes.add(node1);
                nodes.add(node2);
            }

            boolean b = visitor.preVisitEdge(nodes, object);
            if (!b) continue;

            if (node2 != null) visitor.visitEdge(this, node1, node2, object);

            visitor.postVisitEdge(nodes, object);
        }

    }
}
