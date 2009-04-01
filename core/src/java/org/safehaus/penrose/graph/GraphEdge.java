/**
 * Copyright 2009 Red Hat, Inc.
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

import java.util.Collection;
import java.util.HashSet;

/**
 * @author Endi S. Dewata
 */
public class GraphEdge {

    private Collection nodes = new HashSet();
    private Object object;

    public GraphEdge(Object node1, Object node2) {
        this(node1, node2, null);
    }

    public GraphEdge(Object node1, Object node2, Object object) {
        nodes.add(node1);
        nodes.add(node2);
        this.object = object;
    }

    public Collection getNodes() {
        return nodes;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Object getObject() {
        return object;
    }

    public String toString() {
        return "<"+nodes+": "+object+">";
    }
}
