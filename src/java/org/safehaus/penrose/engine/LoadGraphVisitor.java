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
public class LoadGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private AttributeValues sourceValues;
    private Source primarySource;

    private Stack stack = new Stack();

    private Collection results = new ArrayList();
    private AttributeValues loadedSourceValues = new AttributeValues();

    public LoadGraphVisitor(
            Engine engine,
            EntryDefinition entryDefinition,
            AttributeValues sourceValues,
            Filter filter) throws Exception {

        this.engine = engine;
        this.engineContext = engine.getEngineContext();
        this.entryDefinition = entryDefinition;
        this.sourceValues = sourceValues;

        config = engine.getConfig(entryDefinition.getDn());
        graph = engine.getGraph(entryDefinition);
        primarySource = engine.getPrimarySource(entryDefinition);

        Map map = new HashMap();
        map.put("filter", filter);

        stack.push(map);
        //loadedSourceValues.add(sourceValues);
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

        if (sourceValues.contains(source.getName())) {
            log.debug("Source "+source.getName()+" has been loaded.");
            graphIterator.traverseEdges(node);
            return;
        }

        log.debug("Loading source "+source.getName()+" with filter "+filter);

        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection tmp = engineContext.getConnector().search(sourceDefinition, filter);

        Collection list = new ArrayList();
        for (Iterator i=tmp.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            AttributeValues sv = new AttributeValues();
            sv.add(source.getName(), av);
            list.add(sv);

            //sourceValues.add(sv);
        }

        loadedSourceValues.set(source.getName(), list);

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

/*
        Filter filter = null;

        log.debug("Generating filters:");
        for (Iterator i=results.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            Filter f = engine.generateFilter(toSource, relationships, av);
            log.debug(" - "+f);

            filter = engineContext.getFilterTool().appendOrFilter(filter, f);
        }
*/
        Filter filter = engine.generateFilter(toSource, relationships, sourceValues);

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

    public AttributeValues getLoadedSourceValues() {
        return loadedSourceValues;
    }
}
