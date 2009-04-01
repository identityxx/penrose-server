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

/**
 * @author Endi S. Dewata
 */
public class GraphVisitor {

    public boolean preVisitNode(Object node) throws Exception {
        //System.out.println("Pre-visit Node "+node);
        return true;
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {
        graphIterator.traverseEdges(node);
    }

    public void postVisitNode(Object node) throws Exception {
        //System.out.println("Post-visit Node "+node);
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {
        graphIterator.traverse(node2);
    }
}
