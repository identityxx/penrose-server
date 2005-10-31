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
public class SearchCleaner extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private Map filters;
    private Map depths;
    private Source primarySource;

    private Map needCleaning = new HashMap();

    public SearchCleaner(
            Engine engine,
            EntryDefinition entryDefinition,
            SearchPlanner planner,
            Source primarySource) throws Exception {

        this.engine = engine;
        this.engineContext = engine.getEngineContext();
        this.entryDefinition = entryDefinition;
        this.filters = planner.getFilters();
        this.depths = planner.getDepths();
        this.primarySource = primarySource;

        config = engineContext.getConfig(entryDefinition.getDn());
        graph = engine.getGraph(entryDefinition);

        needCleaning.put(primarySource, new Boolean(false));
    }

    public void run(Source source) throws Exception {
        graph.traverse(this, source);
    }

    public void clean(Collection list) throws Exception {

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();
            //log.debug(" - "+av);

            for (Iterator j=entryDefinition.getSources().iterator(); j.hasNext(); ) {
                Source source = (Source)j.next();
                if (needCleaning.get(source) != null) continue;

                av.remove(source.getName());
            }
        }
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;

        log.debug(Formatter.displaySeparator(60));
        log.debug(Formatter.displayLine("Visiting "+source.getName(), 60));
        log.debug(Formatter.displaySeparator(60));

        needCleaning.put(source, new Boolean(false));

        graphIterator.traverseEdges(node);
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

        Integer depth1 = (Integer)depths.get(fromSource);
        Integer depth2 = (Integer)depths.get(toSource);

        if (entryDefinition.getSource(toSource.getName()) == null) {
            log.debug("Source "+toSource.getName()+" is not defined in entry.");
            return;
        }

        if (depth2.intValue() >= depth1.intValue()) {
            log.debug("Source "+toSource.getName()+" is further away from primary source.");
            return;
        }

        graphIterator.traverse(node2);
    }
}
