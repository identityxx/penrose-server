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
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.connector.ConnectionConfig;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LoadGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private PartitionConfig partitionConfig;
    private Graph graph;
    private Engine engine;
    private EntryMapping entryMapping;
    private AttributeValues sourceValues;
    private SourceMapping primarySourceMapping;

    private Stack stack = new Stack();

    private Collection results = new ArrayList();
    private AttributeValues loadedSourceValues = new AttributeValues();

    public LoadGraphVisitor(
            Engine engine,
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            Filter filter) throws Exception {

        this.engine = engine;
        this.entryMapping = entryMapping;
        this.sourceValues = sourceValues;

        partitionConfig = engine.getConfigManager().getConfig(entryMapping);
        graph = engine.getGraph(entryMapping);
        primarySourceMapping = engine.getPrimarySource(entryMapping);

        Map map = new HashMap();
        map.put("filter", filter);

        stack.push(map);
        //loadedSourceValues.add(sourceValues);
    }

    public void run() throws Exception {
        graph.traverse(this, primarySourceMapping);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)node;

        log.debug(Formatter.displaySeparator(60));
        log.debug(Formatter.displayLine("Visiting "+sourceMapping.getName(), 60));
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

        String s = sourceMapping.getParameter(SourceMapping.FILTER);
        if (s != null) {
            Filter sourceFilter = FilterTool.parseFilter(s);
            filter = FilterTool.appendAndFilter(filter, sourceFilter);
        }

        if (sourceValues.contains(sourceMapping.getName())) {
            log.debug("Source "+sourceMapping.getName()+" has been loaded.");
            graphIterator.traverseEdges(node);
            return;
        }

        log.debug("Loading source "+sourceMapping.getName()+" with filter "+filter);

        ConnectionConfig connectionConfig = partitionConfig.getConnectionConfig(sourceMapping.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(sourceMapping.getSourceName());

        SearchResults tmp = engine.getConnector().search(sourceDefinition, filter);

        Collection list = new ArrayList();
        for (Iterator i=tmp.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            AttributeValues sv = new AttributeValues();
            sv.add(sourceMapping.getName(), av);
            list.add(sv);

            //sourceValues.add(sv);
        }

        loadedSourceValues.set(sourceMapping.getName(), list);

        graphIterator.traverseEdges(node);
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        //SourceMapping fromSourceMapping = (SourceMapping)node1;
        SourceMapping toSourceMapping = (SourceMapping)node2;
        Collection relationships = (Collection)object;

        log.debug(Formatter.displaySeparator(60));
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            log.debug(Formatter.displayLine(relationship.toString(), 60));
        }
        log.debug(Formatter.displaySeparator(60));

        if (entryMapping.getSourceMapping(toSourceMapping.getName()) == null) {
            log.debug("Source "+toSourceMapping.getName()+" is not defined in entry "+entryMapping.getDn());
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
        Filter filter = engine.generateFilter(toSourceMapping, relationships, sourceValues);

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
