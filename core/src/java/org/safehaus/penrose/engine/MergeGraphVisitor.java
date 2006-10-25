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
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphIterator;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class MergeGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Partition partition;
    private Graph graph;
    private Engine engine;
    private EntryMapping entryMapping;
    private AttributeValues loadedSourceValues;
    private SourceMapping primarySourceMapping;

    private AttributeValues sourceValues = new AttributeValues();

    private Stack stack = new Stack();

    public MergeGraphVisitor(
            Engine engine,
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues primarySourceValues,
            AttributeValues loadedSourceValues,
            SourceMapping primarySourceMapping,
            Filter filter) throws Exception {

        this.engine = engine;
        this.partition = partition;
        this.entryMapping = entryMapping;
        this.loadedSourceValues = loadedSourceValues;
        this.primarySourceMapping = primarySourceMapping;

        graph = engine.getGraph(entryMapping);

        sourceValues.add(primarySourceValues);

        Map map = new HashMap();
        map.put("filter", filter);

        stack.push(map);
    }

    public void run() throws Exception {
        graph.traverse(this, primarySourceMapping);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)node;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(60));
            log.debug(Formatter.displayLine("Visiting "+sourceMapping.getName(), 60));
            log.debug(Formatter.displaySeparator(60));
        }
/*
        if (source == primarySource) {
            graphIterator.traverseEdges(node);
            return;
        }
*/
        Map map = (Map)stack.peek();
        Filter filter = (Filter)map.get("filter");
        Collection relationships = (Collection)map.get("relationships");

        log.debug("Filter: "+filter);
        log.debug("Relationships: "+relationships);

        String s = sourceMapping.getParameter(SourceMapping.FILTER);
        if (s != null) {
            Filter sourceFilter = FilterTool.parseFilter(s);
            filter = FilterTool.appendAndFilter(filter, sourceFilter);
        }

        if (!sourceValues.contains(sourceMapping.getName())) {

            //log.debug("Loaded values:");
            Collection list = loadedSourceValues.get(sourceMapping.getName());
            if (list != null) {
                for (Iterator i=list.iterator(); i.hasNext(); ) {
                    AttributeValues av = (AttributeValues)i.next();
                    //log.debug(" - "+av);

                    if (relationships == null) {
                        if (!FilterTool.isValid(av, filter)) continue;

                    } else {
                        if (!engine.getJoinEngine().evaluate(partition, entryMapping, relationships, sourceValues, av)) continue;
                    }
    
                    sourceValues.add(av);
                }
            }
        }
/*
        log.debug("Merged source values:");
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }
*/
        graphIterator.traverseEdges(node);
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
            log.debug("Source "+toSourceMapping.getName()+" is not defined in entry "+entryMapping.getDn());
            return;
        }

        Filter filter = engine.generateFilter(toSourceMapping, relationships, sourceValues);

        Map map = new HashMap();
        map.put("filter", filter);
        map.put("relationships", relationships);

        stack.push(map);

        graphIterator.traverse(node2);

        stack.pop();
    }

    public AttributeValues getSourceValues() {
        return sourceValues;
    }
}
