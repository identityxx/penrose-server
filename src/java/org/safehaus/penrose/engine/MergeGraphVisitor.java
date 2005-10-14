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
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.util.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class MergeGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private Collection parentSourceValues;
    private AttributeValues sourceValues;
    private Source primarySource;

    private Collection results = new ArrayList();

    private Stack stack = new Stack();

    public MergeGraphVisitor(
            Config config,
            Graph graph,
            Engine engine,
            EntryDefinition entryDefinition,
            Collection parentSourceValues,
            AttributeValues sourceValues,
            Source primarySource) {

        this.config = config;
        this.graph = graph;
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
        this.entryDefinition = entryDefinition;
        this.parentSourceValues = parentSourceValues;
        this.sourceValues = sourceValues;
        this.primarySource = primarySource;

        results.add(sourceValues);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;

        log.debug(Formatter.displaySeparator(40));
        log.debug(Formatter.displayLine("Merging "+source.getName(), 40));
        log.debug(Formatter.displaySeparator(40));

        if (source == primarySource) {
            graphIterator.traverseEdges(node);
            return;
        }

        Map map = (Map)stack.peek();
        Filter filter = (Filter)map.get("filter");
        Collection relationships = (Collection)map.get("relationships");

        log.debug("Filter: "+filter);
        log.debug("Relationships: "+relationships);

        String s = source.getParameter(Source.FILTER);
        if (s != null) {
            Filter sourceFilter = engineContext.getFilterTool().parseFilter(s);
            filter = engineContext.getFilterTool().appendAndFilter(filter, sourceFilter);
        }

        if (!sourceValues.contains(source.getName())) {

            log.debug("Loading source "+source.getName()+" with filter "+filter);
            Collection tmp = engineContext.getSyncService().search(source, filter);
            Collection list = new ArrayList();
            for (Iterator i=tmp.iterator(); i.hasNext(); ) {
                AttributeValues av = (AttributeValues)i.next();

                AttributeValues sv = new AttributeValues();
                sv.add(source.getName(), av);
                list.add(sv);
            }

            if (results.isEmpty()) {
                results.addAll(list);
            } else {
                Collection temp = engine.getJoinEngine().leftJoin(results, list, relationships);
                results.clear();
                results.addAll(temp);
            }
        }

        log.debug("Total load results:");

        for (Iterator j=results.iterator(); j.hasNext(); ) {
            AttributeValues sv = (AttributeValues)j.next();
            log.debug(" - "+sv);
        }

        graphIterator.traverseEdges(node);
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        Source fromSource = (Source)node1;
        Source toSource = (Source)node2;
        Relationship relationship = (Relationship)object;

        log.debug(Formatter.displaySeparator(40));
        log.debug(Formatter.displayLine(relationship.toString(), 40));
        log.debug(Formatter.displaySeparator(40));

        if (entryDefinition.getSource(toSource.getName()) == null) {
            log.debug("Source "+toSource.getName()+" is not defined in entry "+entryDefinition.getDn());
            return;
        }

        Collection relationships = new ArrayList();
        relationships.add(relationship);

        Filter filter = null;

        log.debug("Generating filters:");
        for (Iterator i=results.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            Filter f = engine.generateFilter(toSource, relationships, av);
            log.debug(" - "+f);

            filter = engineContext.getFilterTool().appendOrFilter(filter, f);
        }

        Map map = new HashMap();
        map.put("filter", filter);
        map.put("relationships", relationships);

        stack.push(map);

        graphIterator.traverse(node2);

        stack.pop();
    }

    public Collection getResults() {
        return results;
    }

    public void setResults(Collection results) {
        this.results = results;
    }
}
