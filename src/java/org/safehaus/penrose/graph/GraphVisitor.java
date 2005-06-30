/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.graph;

/**
 * @author Endi S. Dewata
 */
public class GraphVisitor {

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        //System.out.println("Pre-visit Node "+node);
        return true;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        //System.out.println("Post-visit Node "+node);
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        //System.out.println("Pre-visit Edge <"+node1+", "+node2+"> "+edge);
        return true;
    }

    public void postVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        //System.out.println("Post-visit Edge <"+node1+", "+node2+"> "+edge);
    }
}
