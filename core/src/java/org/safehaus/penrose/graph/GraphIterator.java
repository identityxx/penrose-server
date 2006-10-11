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
package org.safehaus.penrose.graph;

import org.safehaus.penrose.graph.Graph;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class GraphIterator {

    public Graph graph;
    public GraphVisitor visitor;

    private Collection visitedNodes = new HashSet();
    private Collection visitedEdges = new HashSet();

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

    public int traverseEdges(Object node) throws Exception {
        Collection edges = graph.getEdges(node);
        if (edges == null) return 0;

        int counter = 0;
        for (Iterator i=edges.iterator(); i.hasNext(); ) {
            GraphEdge edge = (GraphEdge)i.next();

            if (visitedEdges.contains(edge)) continue;

            visitedEdges.add(edge);
            Object object = edge.getObject();

            Iterator j=edge.getNodes().iterator();
            Object node1 = j.next();
            Object node2 = j.next();

            if (node == node2) { // move from left to right
                Object n = node1;
                node1 = node2;
                node2 = n;
            }

            visitor.visitEdge(this, node1, node2, object);
            counter++;
        }

        return counter;
    }

    public Collection getVisitedNodes() {
        return visitedNodes;
    }

    public Collection getVisitedEdges() {
        return visitedEdges;
    }
}
