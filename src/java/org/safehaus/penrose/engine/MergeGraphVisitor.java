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
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.util.Formatter;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class MergeGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private AttributeValues primarySourceValues;
    private AttributeValues loadedSourceValues;
    private Source primarySource;

    private AttributeValues sourceValues = new AttributeValues();

    private Stack stack = new Stack();

    public MergeGraphVisitor(
            Engine engine,
            EntryDefinition entryDefinition,
            AttributeValues primarySourceValues,
            AttributeValues loadedSourceValues,
            Source primarySource,
            Filter filter) throws Exception {

        this.engine = engine;
        this.engineContext = engine.getEngineContext();
        this.entryDefinition = entryDefinition;
        this.primarySourceValues = primarySourceValues;
        this.loadedSourceValues = loadedSourceValues;
        this.primarySource = primarySource;

        config = engineContext.getConfig(entryDefinition.getDn());
        graph = engine.getGraph(entryDefinition);

        sourceValues.add(primarySourceValues);

        Map map = new HashMap();
        map.put("filter", filter);

        stack.push(map);
    }

    public void run() throws Exception {
        graph.traverse(this, primarySource);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;

        log.debug(Formatter.displaySeparator(60));
        log.debug(Formatter.displayLine("Visiting "+source.getName(), 60));
        log.debug(Formatter.displaySeparator(60));
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

        String s = source.getParameter(Source.FILTER);
        if (s != null) {
            Filter sourceFilter = engineContext.getFilterTool().parseFilter(s);
            filter = FilterTool.appendAndFilter(filter, sourceFilter);
        }

        if (!sourceValues.contains(source.getName())) {

            //log.debug("Loaded values:");
            Collection list = loadedSourceValues.get(source.getName());

            for (Iterator i=list.iterator(); i.hasNext(); ) {
                AttributeValues av = (AttributeValues)i.next();
                //log.debug(" - "+av);

                if (relationships == null) {
                    if (!engineContext.getFilterTool().isValidEntry(av, filter)) continue;

                } else {
                    if (!engine.getJoinEngine().evaluate(relationships, sourceValues, av)) continue;
                }

                sourceValues.add(av);
            }
        }

        log.debug("Source values:");
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

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

        if (entryDefinition.getSource(toSource.getName()) == null) {
            log.debug("Source "+toSource.getName()+" is not defined in entry "+entryDefinition.getDn());
            return;
        }

        Filter filter = engine.generateFilter(toSource, relationships, sourceValues);

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
