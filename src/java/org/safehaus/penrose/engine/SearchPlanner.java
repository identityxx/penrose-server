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
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphIterator;
import org.safehaus.penrose.util.Formatter;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchPlanner extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EntryDefinition entryDefinition;
    private Filter searchFilter;
    private Source primarySource;
    private AttributeValues sourceValues;

    private Stack depthStack = new Stack();
    private Stack sourceStack = new Stack();

    private Map filters = new HashMap();
    private Map depths = new HashMap();
    
    private Collection connectingRelationships = new ArrayList();
    private Collection connectingSources = new ArrayList();

    public SearchPlanner(
            Engine engine,
            EntryDefinition entryDefinition,
            Filter filter,
            AttributeValues sourceValues) throws Exception {

        this.engine = engine;
        this.entryDefinition = entryDefinition;
        this.searchFilter = filter;
        this.sourceValues = sourceValues;

        config = engine.getConfig(entryDefinition.getDn());
        graph = engine.getGraph(entryDefinition);
        primarySource = engine.getPrimarySource(entryDefinition);                          
    }

    public void run() throws Exception {

        depthStack.push(new Integer(0));
        graph.traverse(this, primarySource);

        log.debug("Source depths and filters:");
        for (Iterator i=entryDefinition.getSources().iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            Integer depth = (Integer)depths.get(source);
            Filter filter = (Filter)filters.get(source);
            log.debug(" - "+source.getName()+": "+filter+" ("+depth+")");
        }

        log.debug("Connecting sources:");
        for (Iterator i=connectingRelationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            log.debug(" - "+relationship);

            Collection relationships = new ArrayList();
            relationships.add(relationship);

            String lhs = relationship.getLhs();
            int lindex = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, lindex);

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, rindex);

            Source fromSource;
            Source toSource;

            if (entryDefinition.getSource(lsourceName) == null) {
                fromSource = config.getEffectiveSource(entryDefinition, lsourceName);
                toSource = entryDefinition.getSource(rsourceName);

            } else {
                fromSource = config.getEffectiveSource(entryDefinition, rsourceName);
                toSource = entryDefinition.getSource(lsourceName);
            }

            Map map = new HashMap();
            map.put("fromSource", fromSource);
            map.put("toSource", toSource);
            map.put("relationships", relationships);

            connectingSources.add(map);
        }
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;

        log.debug(Formatter.displaySeparator(60));
        log.debug(Formatter.displayLine("Visiting "+source.getName(), 60));
        log.debug(Formatter.displaySeparator(60));

        Integer depth = (Integer)depthStack.peek();

        Filter sourceFilter = engine.getFilterTool().toSourceFilter(null, entryDefinition, source, searchFilter);
        log.debug("Filter: "+sourceFilter+" ("+(sourceFilter == null ? "null" : "not null")+")");
        log.debug("Depth: "+depth);

        filters.put(source, sourceFilter);
        depths.put(source, depth);

        depthStack.push(new Integer(depth.intValue()+1));
        sourceStack.push(source);

        graphIterator.traverseEdges(node);

        sourceStack.pop();
        depthStack.pop();
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        Source fromSource = (Source)node1;
        Source toSource = (Source)node2;
        Collection relationships = (Collection)object;

        log.debug(Formatter.displaySeparator(60));
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            log.debug(Formatter.displayLine(relationship.toString(), 60));
        }
        log.debug(Formatter.displaySeparator(60));

        if (entryDefinition.getSource(toSource.getName()) == null) {
            log.debug("Source "+toSource.getName()+" is not defined in entry "+entryDefinition.getDn());
            connectingRelationships.addAll(relationships);
            
            return;
        }

        graphIterator.traverse(node2);
    }

    public Map getDepths() {
        return depths;
    }

    public Map getFilters() {
        return filters;
    }

    public Source getFirstSource() {
        Source source = null;
        int maxDepth = -1;

        for (Iterator i=entryDefinition.getSources().iterator(); i.hasNext(); ) {
            Source s = (Source)i.next();

            Integer depth = (Integer)depths.get(s);
            if (depth == null) continue;

            int d = depth.intValue();
            Filter f = (Filter)filters.get(s);

            //log.debug("Comparing with "+s.getName()+": "+f+" ("+d+")");

            if (source == null || (d > maxDepth && f != null)) {
                source = s;
                maxDepth = d;
                //log.debug("Selecting "+s.getName()+": "+f+" ("+d+")");
            }
        }

        return source;
    }

    public Collection getConnectingRelationships() {
        return connectingRelationships;
    }

    public Collection getConnectingSources() {
        return connectingSources;
    }
}
