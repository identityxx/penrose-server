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
import org.safehaus.penrose.graph.GraphIterator;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchCleaner extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Partition partition;
    private Graph graph;
    private Engine engine;
    private EntryMapping entryMapping;
    private Map filters;
    private Map depths;
    private SourceMapping primarySourceMapping;

    private Map needCleaning = new HashMap();

    public SearchCleaner(
            Engine engine,
            Partition partition,
            EntryMapping entryMapping,
            SearchPlanner planner,
            SourceMapping primarySourceMapping) throws Exception {

        this.engine = engine;
        this.partition = partition;
        this.entryMapping = entryMapping;
        this.filters = planner.getFilters();
        this.depths = planner.getDepths();
        this.primarySourceMapping = primarySourceMapping;

        graph = engine.getGraph(entryMapping);

        needCleaning.put(primarySourceMapping, new Boolean(false));
    }

    public void run(SourceMapping sourceMapping) throws Exception {
        graph.traverse(this, sourceMapping);
    }

    public void clean(Collection list) throws Exception {

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();
            //log.debug(" - "+av);

            for (Iterator j=entryMapping.getSourceMappings().iterator(); j.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)j.next();
                if (needCleaning.get(sourceMapping) != null) continue;
                log.debug("Removing results from source "+sourceMapping.getName());

                av.remove(sourceMapping.getName());
            }
        }
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)node;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(60));
            log.debug(Formatter.displayLine("Visiting "+sourceMapping.getName(), 60));
            log.debug(Formatter.displaySeparator(60));
        }

        needCleaning.put(sourceMapping, new Boolean(false));

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

        Integer depth1 = (Integer)depths.get(fromSourceMapping);
        Integer depth2 = (Integer)depths.get(toSourceMapping);

        if (entryMapping.getSourceMapping(toSourceMapping.getName()) == null) {
            log.debug("Source "+toSourceMapping.getName()+" is not defined in entry.");
            return;
        }

        if (depth2.intValue() >= depth1.intValue()) {
            log.debug("Source "+toSourceMapping.getName()+" is further away from primary source.");
            return;
        }

        graphIterator.traverse(node2);
    }
}
