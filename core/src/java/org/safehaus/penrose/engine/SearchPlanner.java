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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphIterator;
import org.safehaus.penrose.util.Formatter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchPlanner extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Partition partition;
    private Graph graph;
    private Engine engine;
    private EntryMapping entryMapping;
    private Filter searchFilter;
    private SourceMapping primarySourceMapping;
    private AttributeValues sourceValues;

    private Stack depthStack = new Stack();
    private Stack sourceStack = new Stack();

    private Map filters = new HashMap();
    private Map depths = new HashMap();
    
    private Collection connectingRelationships = new ArrayList();
    private Collection connectingSources = new ArrayList();

    public SearchPlanner(
            Engine engine,
            Partition partition,
            EntryMapping entryMapping,
            Filter filter,
            AttributeValues sourceValues) throws Exception {

        this.engine = engine;
        this.partition = partition;
        this.entryMapping = entryMapping;
        this.searchFilter = filter;
        this.sourceValues = sourceValues;

        graph = engine.getGraph(entryMapping);
        primarySourceMapping = engine.getPrimarySource(entryMapping);
    }

    public void run() throws Exception {

        depthStack.push(new Integer(0));

        if (primarySourceMapping != null) {
            log.debug("Primary source: "+primarySourceMapping.getName());
            graph.traverse(this, primarySourceMapping);
        }

        log.debug("Source depths and filters:");
        for (Iterator i=entryMapping.getSourceMappings().iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            Integer depth = (Integer)depths.get(sourceMapping);
            Filter filter = (Filter)filters.get(sourceMapping);
            log.debug(" - "+sourceMapping.getName()+": "+filter+" ("+depth+")");
        }

        log.debug("Connecting sources:");
        for (Iterator i=connectingRelationships.iterator(); i.hasNext(); ) {
            Collection relationships = (Collection)i.next();

            Relationship relationship = (Relationship)relationships.iterator().next();
            log.debug(" - "+relationship);

            String lhs = relationship.getLhs();
            int lindex = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, lindex);

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, rindex);

            SourceMapping fromSourceMapping;
            SourceMapping toSourceMapping;

            if (entryMapping.getSourceMapping(lsourceName) == null) {
                fromSourceMapping = partition.getEffectiveSourceMapping(entryMapping, lsourceName);
                toSourceMapping = entryMapping.getSourceMapping(rsourceName);

            } else {
                fromSourceMapping = partition.getEffectiveSourceMapping(entryMapping, rsourceName);
                toSourceMapping = entryMapping.getSourceMapping(lsourceName);
            }

            Map map = new HashMap();
            map.put("fromSource", fromSourceMapping);
            map.put("toSource", toSourceMapping);
            map.put("relationships", relationships);

            connectingSources.add(map);
        }
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)node;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(60));
            log.debug(Formatter.displayLine("Visiting "+sourceMapping.getName(), 60));
            log.debug(Formatter.displaySeparator(60));
        }

        Integer depth = (Integer)depthStack.peek();

        Filter sourceFilter = engine.getEngineFilterTool().toSourceFilter(partition, null, entryMapping, sourceMapping, searchFilter);
        log.debug("Filter: "+sourceFilter+" ("+(sourceFilter == null ? "null" : "not null")+")");
        log.debug("Depth: "+depth);

        filters.put(sourceMapping, sourceFilter);
        depths.put(sourceMapping, depth);

        depthStack.push(new Integer(depth.intValue()+1));
        sourceStack.push(sourceMapping);

        graphIterator.traverseEdges(node);

        sourceStack.pop();
        depthStack.pop();
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        SourceMapping fromSourceMapping = (SourceMapping)node1;
        SourceMapping toSourceMapping = (SourceMapping)node2;
        Collection relationships = (Collection)object;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(60));
            for (Iterator i=relationships.iterator(); i.hasNext(); ) {
                Relationship relationship = (Relationship)i.next();
                log.debug(Formatter.displayLine(relationship.toString(), 60));
            }
            log.debug(Formatter.displaySeparator(60));
        }
        
        if (entryMapping.getSourceMapping(toSourceMapping.getName()) == null) {
            log.debug("Source "+toSourceMapping.getName()+" is inherited");
            connectingRelationships.add(relationships);
            
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

    public SourceMapping getFirstSource() {
        SourceMapping sourceMapping = null;
        int maxDepth = -1;

        for (Iterator i=entryMapping.getSourceMappings().iterator(); i.hasNext(); ) {
            SourceMapping s = (SourceMapping)i.next();

            Integer depth = (Integer)depths.get(s);
            if (depth == null) continue;

            int d = depth.intValue();
            Filter f = (Filter)filters.get(s);

            //log.debug("Comparing with "+s.getName()+": "+f+" ("+d+")");

            if (sourceMapping == null || (d > maxDepth && f != null)) {
                sourceMapping = s;
                maxDepth = d;
                //log.debug("Selecting "+s.getName()+": "+f+" ("+d+")");
            }
        }

        return sourceMapping;
    }

    public Collection getConnectingRelationships() {
        return connectingRelationships;
    }

    public Collection getConnectingSources() {
        return connectingSources;
    }
}
