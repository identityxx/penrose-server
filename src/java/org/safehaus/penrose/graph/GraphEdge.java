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

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class GraphEdge {

    private Collection nodes = new ArrayList();
    private Collection objects = new ArrayList();

    public void addNode(Object node) {
        nodes.add(node);
    }

    public Collection getNodes() {
        return nodes;
    }

    public void removeNode(Object node) {
        nodes.remove(node);
    }

    public void addObject(Object object) {
        objects.add(object);
    }

    public Collection getObjects() {
        return objects;
    }

    public void removeObject(Object object) {
        objects.remove(object);
    }
}
