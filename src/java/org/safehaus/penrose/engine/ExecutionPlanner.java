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
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ExecutionPlanner extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private Config config;
    private Graph graph;
    private EntryDefinition entryDefinition;

    private Collection sources = new ArrayList();
    private Map dependencies = new HashMap();
    private Map filters = new HashMap();

    public ExecutionPlanner(
            Config config,
            Graph graph,
            EntryDefinition entryDefinition) throws Exception {

        this.config = config;
        this.graph = graph;
        this.entryDefinition = entryDefinition;
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;

        if (entryDefinition.getSource(source.getName()) == null) return true;

        sources.add(source);

        Collection relationships = graph.getEdgeObjects(source);

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            if (isJoinRelationship(relationship)) continue;

            Collection list = (Collection)filters.get(source);
            if (list == null) {
                list = new ArrayList();
                filters.put(source, list);
            }
            list.add(relationship);
        }

        return true;
    }

    public boolean isJoinRelationship(Relationship relationship) {
        Collection operands = relationship.getOperands();
        if (operands.size() < 2) return false;

        int counter = 0;
        for (Iterator j=operands.iterator(); j.hasNext(); ) {
            String operand = j.next().toString();

            int index = operand.indexOf(".");
            if (index < 0) continue;

            String sourceName = operand.substring(0, index);
            Source src = config.getEffectiveSource(entryDefinition, sourceName);
            if (src == null) continue;

            counter++;
        }

        if (counter < 2) return false;

        return true;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
    }

    public boolean preVisitEdge(Collection nodes, Object object, Object parameter) throws Exception {

        Relationship relationship = (Relationship)object;
        if (!isJoinRelationship(relationship)) return false;

        Iterator iterator = nodes.iterator();
        Source fromSource = (Source)iterator.next();
        Source toSource = (Source)iterator.next();

        if (entryDefinition.getSource(toSource.getName()) == null) return false;

        Collection list = (Collection)dependencies.get(toSource);
        if (list == null) {
            list = new ArrayList();
            dependencies.put(toSource, list);
        }
        list.add(fromSource);

        return true;
    }

    public void postVisitEdge(Collection nodes, Object object, Object parameter) throws Exception {
    }

    public EntryDefinition getEntryDefinition() {
        return entryDefinition;
    }

    public void setEntryDefinition(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    public Collection getSources() {
        return sources;
    }

    public void setSources(Collection sources) {
        this.sources = sources;
    }

    public Map getDependencies() {
        return dependencies;
    }

    public void setDependencies(Map dependencies) {
        this.dependencies = dependencies;
    }

    public Map getFilters() {
        return filters;
    }

    public void setFilters(Map filters) {
        this.filters = filters;
    }
}
