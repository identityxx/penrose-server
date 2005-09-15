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
