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

    public void addEdge(GraphEdge edge) throws Exception {
        for (Iterator i=edge.getNodes().iterator(); i.hasNext(); ) {
            Object node = i.next();
            if (!nodes.contains(node)) throw new Exception("Node "+node+" is not in the graph.");
        }

        edges.put(edge.getNodes(), edge);

        for (Iterator i=edge.getNodes().iterator(); i.hasNext(); ) {
            Object node = i.next();

            Set set = (Set)indices.get(node);
            if (set == null) {
                set = new HashSet();
                indices.put(node, set);
            }

            set.add(edge);
        }
    }

    public GraphEdge getEdge(Set nodes) {
        return (GraphEdge)edges.get(nodes);
    }
    
    public Collection getEdges() {
        return edges.values();
    }

    public Collection getEdges(Object node) throws Exception {
        if (!nodes.contains(node)) throw new Exception("Node "+node+" is not in the graph.");
        return (Collection)indices.get(node);
    }

    public Collection getEdgeObjects(Object node) throws Exception {
        Collection objects = new ArrayList();

        Collection set = getEdges(node);
        if (set == null) return objects;

        for (Iterator i=set.iterator(); i.hasNext(); ) {
            GraphEdge edge = (GraphEdge)i.next();
            Object object = edge.getObject();
            objects.add(object);
        }

        return objects;
    }

    public String toString() {
        return "Nodes: "+nodes+", Edges: "+edges;
    }

    public void traverse(GraphVisitor visitor) throws Exception {
        Object node = getNodes().iterator().next();
        traverse(visitor, node);
    }

    public void traverse(GraphVisitor visitor, Object node) throws Exception {
        new GraphIterator(this, visitor).traverse(node);
    }
}
