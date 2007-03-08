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
package org.safehaus.penrose.engine.impl;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.graph.GraphIterator;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.DefaultEngine;
import org.safehaus.penrose.entry.AttributeValues;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchParentRunner extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Partition partition;
    private Graph graph;
    private EngineImpl engine;
    private EntryMapping entryMapping;
    private AttributeValues sourceValues;
    private Map filters;
    private SourceMapping startingSourceMapping;

    private Stack stack = new Stack();
    
    private Collection results;

    public SearchParentRunner(
            EngineImpl engine,
            Partition partition,
            EntryMapping entryMapping,
            Collection results,
            AttributeValues sourceValues,
            SearchPlanner planner,
            SourceMapping startingSourceMapping,
            Collection relationships) throws Exception {

        this.engine = engine;
        this.partition = partition;
        this.entryMapping = entryMapping;
        this.filters = planner.getFilters();
        this.startingSourceMapping = startingSourceMapping;
        this.results = results;
        this.sourceValues = sourceValues;

        graph = engine.getGraph(entryMapping);

        Filter filter = (Filter)filters.get(startingSourceMapping);

        Map map = new HashMap();
        map.put("filter", filter);
        map.put("relationships", relationships);

        stack.push(map);

    }

    public void run() throws Exception {
        graph.traverse(this, startingSourceMapping);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)node;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(60));
            log.debug(Formatter.displayLine("Visiting "+sourceMapping.getName(), 60));
            log.debug(Formatter.displaySeparator(60));
        }

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

            log.debug("Source "+sourceMapping.getName()+" has already been searched");

            Collection list = new ArrayList();
            for (Iterator i=results.iterator(); i.hasNext(); ) {
                AttributeValues av = (AttributeValues)i.next();

                if (relationships == null) {
                    if (!FilterTool.isValid(av, filter)) continue;

                } else {
                    if (!engine.joinEngine.evaluate(partition, entryMapping, relationships, av, av)) continue;
                }

                list.add(av);
            }

            results.clear();
            results.addAll(list);

        } else {

            log.debug("Searching source "+sourceMapping.getName()+" with filter "+filter);

            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

            PenroseSearchControls sc = new PenroseSearchControls();
            PenroseSearchResults tmp = new PenroseSearchResults();
            
            Connector connector = engine.getConnector(sourceConfig);
            connector.search(
                    partition,
                    entryMapping,
                    sourceMapping,
                    sourceConfig,
                    null,
                    filter,
                    sc,
                    tmp
            );

            Collection list = new ArrayList();
            while (tmp.hasNext()) {
                AttributeValues av = (AttributeValues)tmp.next();

                AttributeValues sv = new AttributeValues();
                sv.add(sourceMapping.getName(), av);
                list.add(sv);
            }

            if (results.isEmpty()) {
                results.addAll(list);

            } else {
                Collection temp;
                if (sourceMapping.isRequired()) {
                    temp = engine.joinEngine.join(results, list, partition, entryMapping, relationships);
                } else {
                    temp = engine.joinEngine.leftJoin(results, list, partition, entryMapping, relationships);
                }
                results.clear();
                results.addAll(temp);
            }
        }
/*
        log.debug("Search Results:");

        int counter = 1;
        for (Iterator i=results.iterator(); i.hasNext(); counter++) {
            AttributeValues sv = (AttributeValues)i.next();
            log.debug("Record #"+counter+":");
            for (Iterator k=sv.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                Collection values = sv.get(name);
                log.debug(" - "+name+": "+values);
            }
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

        if (entryMapping.getSourceMapping(toSourceMapping.getName()) != null) {
            log.debug("Source "+toSourceMapping.getName()+" is not defined in parent entry.");
            return;
        }

        Filter filter = null;

        //log.debug("Generating filters:");
        for (Iterator i=results.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            Filter f = engine.generateFilter(toSourceMapping, relationships, av);
            //log.debug(" - "+f);

            filter = FilterTool.appendOrFilter(filter, f);
        }

        Filter sourceFilter = (Filter)filters.get(toSourceMapping);
        filter = FilterTool.appendAndFilter(filter, sourceFilter);

        if (filter == null) return;

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
}
