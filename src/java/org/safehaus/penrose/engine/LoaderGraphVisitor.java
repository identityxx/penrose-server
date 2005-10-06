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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LoaderGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private AttributeValues sourceValues;

    private Stack stack = new Stack();

    public LoaderGraphVisitor(
            Config config,
            Graph graph,
            Engine engine,
            EntryDefinition entryDefinition,
            AttributeValues sourceValues,
            Object object) {

        this.config = config;
        this.graph = graph;
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
        this.entryDefinition = entryDefinition;
        this.sourceValues = sourceValues;

        stack.push(object);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;
        Object object = stack.peek();
        log.debug("Loading "+source.getName()+" with "+object);

        //if (entryDefinition.getSource(source.getName()) == null) {
        //    log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
        //    return;
        //}

        Collection results = new ArrayList();

        if (entryDefinition.getSource(source.getName()) == null && sourceValues.contains(source.getName())) {
            Collection list = engine.getEngineContext().getTransformEngine().convert(sourceValues);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();
                log.debug(" - "+row);
                results.add(row);
            }

        } else {

            Collection values;
            if (object instanceof Filter) {
                Filter filter = (Filter)object;
                values = engineContext.getSyncService().search(source, filter);

            } else if (object instanceof Collection) {
                Collection pks = (Collection)object;
                values = engineContext.getSyncService().load(source, pks);

            } else {
                return;
            }

            if (values.size() == 0) return;

            log.debug("Records:");
            for (Iterator i=values.iterator(); i.hasNext(); ) {
                AttributeValues av = (AttributeValues)i.next();

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
                    log.debug(" - "+newRow);
                    results.add(newRow);
                }
            }
        }

        stack.push(results);

        graphIterator.traverseEdges(node);

        stack.pop();
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        Relationship relationship = (Relationship)object;
        log.debug("Relationship "+relationship);

        Source fromSource = (Source)node1;
        Source toSource = (Source)node2;

        //if (entryDefinition.getSource(toSource.getName()) == null) {
        //    log.debug("Source "+toSource.getName()+" is not defined in entry "+entryDefinition.getDn());
        //    return;
        //}

        Collection rows = (Collection)stack.peek();

        Collection relationships = new ArrayList();
        relationships.add(relationship);

        Filter filter = engine.generateFilter(toSource, relationships, rows);
        if (filter == null) return;

        stack.push(filter);

        graphIterator.traverse(node2);

        stack.pop();
    }
}
