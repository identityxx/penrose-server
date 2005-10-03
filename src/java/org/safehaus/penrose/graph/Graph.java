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

    public void addEdge(Collection edge, Object object) throws Exception {
        for (Iterator i=edge.iterator(); i.hasNext(); ) {
            Object node = i.next();
            if (!nodes.contains(node)) throw new Exception("Node "+node+" is not in the graph.");
        }

        edges.put(edge, object);

        for (Iterator i=edge.iterator(); i.hasNext(); ) {
            Object node = i.next();

            Set index = (Set)indices.get(node);
            if (index == null) {
                index = new HashSet();
                indices.put(node, index);
            }

            index.add(edge);
        }
    }

    public Collection getEdges() {
        return edges.keySet();
    }

    public Collection getEdges(Object node) throws Exception {
        if (!nodes.contains(node)) throw new Exception("Node "+node+" is not in the graph.");
        return (Collection)indices.get(node);
    }

    public Collection getEdgeObjects(Object node) throws Exception {
        Collection objects = new ArrayList();

        Collection list = getEdges(node);
        if (list == null) return objects;

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Collection edge = (Set)i.next();
            Object object = edges.get(edge);
            objects.add(object);
        }

        return objects;
    }

    public Object getEdgeObject(Set edge) throws Exception {
        for (Iterator i=edge.iterator(); i.hasNext(); ) {
            Object node = i.next();
            if (!nodes.contains(node)) throw new Exception("Node "+node+" is not in the graph.");
        }

        return edges.get(edge);
    }

    public String toString() {
        return "Nodes: "+nodes+", Edges: "+edges.keySet();
    }

    public void traverse(GraphVisitor visitor) throws Exception {
        Object node = getNodes().iterator().next();
        traverse(visitor, node);
    }

    public void traverse(GraphVisitor visitor, Object node) throws Exception {
        new GraphIterator(this, visitor).traverse(node);
    }
}
