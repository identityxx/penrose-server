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
public class SearchGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private Collection parentSourceValues;
    private Filter searchFilter;
    private Source primarySource;

    private Stack stack = new Stack();
    private Set results = new HashSet();

    public SearchGraphVisitor(
            Config config,
            Graph graph,
            Engine engine,
            EntryDefinition entryDefinition,
            Collection parentSourceValues,
            Collection filters,
            Filter searchFilter,
            Source primarySource) throws Exception {

        this.config = config;
        this.graph = graph;
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
        this.entryDefinition = entryDefinition;
        this.parentSourceValues = parentSourceValues;
        this.searchFilter = searchFilter;
        this.primarySource = primarySource;

        stack.push(filters);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;
        Collection filters = (Collection)stack.peek();

        log.debug(Formatter.displaySeparator(40));
        log.debug(Formatter.displayLine("Searching "+source.getName(), 40));
        log.debug(Formatter.displaySeparator(40));

        Collection values = new ArrayList();

        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Map map = (Map)i.next();

            AttributeValues attributeValues = (AttributeValues)map.get("attributeValues");
            Filter filter = (Filter)map.get("filter");
            log.debug(" - filter: "+filter);

            Collection list;
            if (entryDefinition.getSource(source.getName()) == null) {
                list = new ArrayList();
                for (Iterator j=parentSourceValues.iterator(); j.hasNext(); ) {
                    AttributeValues sourceValues = (AttributeValues)j.next();

                    AttributeValues av = new AttributeValues();
                    for (Iterator k=sourceValues.getNames().iterator(); k.hasNext(); ) {
                        String name = (String)k.next();
                        if (!name.startsWith(source.getName()+".")) continue;

                        int index = name.indexOf(".");
                        String fieldName = name.substring(index+1);

                        Collection v = sourceValues.get(name);
                        av.set(fieldName, v);
                    }

                    list.add(av);
                }

            } else {
                Filter f = engine.getFilterTool().toSourceFilter(null, entryDefinition, source, searchFilter);

                filter = engineContext.getFilterTool().appendAndFilter(filter, f);

                String s = source.getParameter(Source.FILTER);
                if (s != null) {
                    Filter sourceFilter = engineContext.getFilterTool().parseFilter(s);
                    filter = engineContext.getFilterTool().appendAndFilter(filter, sourceFilter);
                }

                log.debug("Searching source "+source.getName()+" with filter "+filter);

                list = engineContext.getSyncService().search(source, filter);
            }

            log.debug("Searching results:");
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                AttributeValues av = (AttributeValues)j.next();
                log.debug(" - "+av);

                AttributeValues newAttributeValues = new AttributeValues();
                newAttributeValues.add(attributeValues);
                newAttributeValues.add(source.getName(), av);

                values.add(newAttributeValues);
                //log.debug(" - "+newAttributeValues);
            }
        }
/*
        Collection results = new ArrayList();

        if (entryDefinition.getSource(source.getName()) == null && sourceValues.contains(source.getName())) {
            log.debug("Source "+source.getName()+" has been searched:");

            Collection list = engine.getEngineContext().getTransformEngine().convert(sourceValues);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();
                log.debug(" - "+row);
                results.add(row);
            }

        } else {

            Collection values;

            Filter f = engine.getFilterTool().toSourceFilter(null, entryDefinition, source, searchFilter);

            filter = engineContext.getFilterTool().appendAndFilter(filter, f);

            String s = source.getParameter(Source.FILTER);
            if (s != null) {
                Filter sourceFilter = engineContext.getFilterTool().parseFilter(s);
                filter = engineContext.getFilterTool().appendAndFilter(filter, sourceFilter);
            }

            log.debug("Searching source "+source.getName()+" with filter "+filter);

            values = engine.getEngineContext().getSyncService().search(source, filter);

            if (values.size() == 0) return;

            log.debug("Found "+values.size()+" record(s):");
            for (Iterator i=values.iterator(); i.hasNext(); ) {
                AttributeValues av = (AttributeValues)i.next();
                log.debug(" - "+av);
                Collection list = engine.getEngineContext().getTransformEngine().convert(av);
                for (Iterator j=list.iterator(); j.hasNext(); ) {
                    Row row = (Row)j.next();
                    Row newRow = new Row();
                    for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                        String name = (String)k.next();
                        Object value = row.get(name);
                        if (value == null) continue;
                        newRow.set(source.getName()+"."+name, value);
                    }
                    log.debug("   - Row: "+newRow);
                    results.add(newRow);
                }
            }
        }
*/
        if (source == primarySource) {
            log.debug("Source "+source.getName()+" is the primary source of entry "+entryDefinition.getDn());
            results.addAll(values);
            return;
        }

        stack.push(values);

        graphIterator.traverseEdges(node);

        stack.pop();
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        Source fromSource = (Source)node1;
        Source toSource = (Source)node2;
        Relationship relationship = (Relationship)object;

        log.debug(Formatter.displaySeparator(40));
        log.debug(Formatter.displayLine(relationship.toString(), 40));
        log.debug(Formatter.displaySeparator(40));

        //if (entryDefinition.getSource(toSource.getName()) == null) {
        //    log.debug("Source "+toSource.getName()+" is not defined in entry "+entryDefinition.getDn());
        //    return;
        //}

        Collection relationships = new ArrayList();
        relationships.add(relationship);

        Collection values = (Collection)stack.peek();
        Collection filters = new ArrayList();
        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            Filter filter = engine.generateFilter(toSource, relationships, av);
            //Filter filter = engine.generateFilter(toSource, relationships, rows);

            log.debug(" - filter: "+filter);
            log.debug("   attribute values: "+av);
            //if (filter == null) continue;

            Map map = new HashMap();
            map.put("attributeValues", av);
            map.put("filter", filter);

            filters.add(map);
        }

        stack.push(filters);

        graphIterator.traverse(node2);

        stack.pop();
    }

    public Set getResults() {
        return results;
    }

    public void setResults(Set results) {
        this.results = results;
    }
}
