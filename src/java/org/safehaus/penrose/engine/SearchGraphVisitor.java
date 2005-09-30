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
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private Config config;
    private Graph graph;
    private Engine engine;
    private EntryDefinition entryDefinition;
    private Source primarySource;

    private Stack stack = new Stack();
    private Set keys = new HashSet();

    public SearchGraphVisitor(
            Config config,
            Graph graph,
            Engine engine,
            EntryDefinition entryDefinition,
            Object object,
            Source primarySource) throws Exception {

        this.config = config;
        this.graph = graph;
        this.engine = engine;
        this.entryDefinition = entryDefinition;
        this.primarySource = primarySource;

        stack.push(object);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            return true;
        }

        Filter filter = (Filter)stack.peek();
        log.debug("Searching "+source.getName()+" for: "+filter);

        Collection relationships = graph.getEdgeObjects(source);

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            if (isJoinRelationship(relationship)) continue;

            Collection operands = relationship.getOperands();
            Iterator iterator = operands.iterator();

            String operand = iterator.next().toString();
            int index = operand.indexOf(".");
            String attribute = operand.substring(index+1);

            String value = iterator.next().toString();

            SimpleFilter sf = new SimpleFilter(attribute, relationship.getOperator(), value);

            log.debug("Filter with "+sf);
            if (filter == null) {
                filter = sf;

            } else if (filter instanceof AndFilter) {
                AndFilter andFilter = (AndFilter)filter;
                andFilter.addFilterList(sf);

            } else {
                AndFilter andFilter = new AndFilter();
                andFilter.addFilterList(filter);
                andFilter.addFilterList(sf);
                filter = andFilter;
            }
        }

        Map map = engine.getEngineContext().getSyncService().search(source, filter);
        if (map.size() == 0) return false;

        log.debug("Records:");
        Collection results = new ArrayList();
        for (Iterator i=map.values().iterator(); i.hasNext(); ) {
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

        stack.push(results);

        keys.addAll(results);

        if (source != primarySource) {
            log.debug("Source "+source.getName()+" is not the primary source of entry "+entryDefinition.getDn());
            return true;
        }

        return false;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        stack.pop();
    }

    public boolean isJoinRelationship(Relationship relationship) {
        Collection operands = relationship.getOperands();
        if (operands.size() < 2) return false;

        int counter = 0;
        for (Iterator j=operands.iterator(); j.hasNext(); ) {
            String operand = j.next().toString();

            int index = operand.indexOf(".");
            if (index < 0) continue;

            String sourceName = operand.substring(0, index);
            Source src = config.getEffectiveSource(entryDefinition, sourceName);
            if (src == null) continue;

            counter++;
        }

        if (counter < 2) return false;

        return true;
    }

    public boolean preVisitEdge(Collection nodes, Object object, Object parameter) throws Exception {
        Relationship relationship = (Relationship)object;
        if (!isJoinRelationship(relationship)) return false;
        log.debug("Relationship "+relationship);

        Iterator iterator = nodes.iterator();
        Source fromSource = (Source)iterator.next();
        Source toSource = (Source)iterator.next();

        Collection rows = (Collection)stack.peek();

        String lhs = relationship.getLhs();
        String operator = relationship.getOperator();
        String rhs = relationship.getRhs();

        if (rhs.startsWith(toSource.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        Collection newRows = new HashSet();
        Filter filter = null;

        log.debug("Primary keys:");
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            Object value = row.get(rhs);
            if (value == null) continue;

            int index = lhs.indexOf(".");
            String name = lhs.substring(index+1);

            SimpleFilter sf = new SimpleFilter(name, operator, value.toString());
            log.debug("   - "+rhs+" -> "+sf);

            if (filter == null) {
                filter = sf;

            } else if (filter instanceof OrFilter) {
                OrFilter of = (OrFilter)filter;
                of.addFilterList(sf);

            } else {
                OrFilter of = new OrFilter();
                of.addFilterList(filter);
                of.addFilterList(sf);
                filter = of;
            }

            Row newRow = new Row();
            newRow.set(lhs, value);
            newRows.add(newRow);
        }

        if (newRows.size() == 0) return false;

        stack.push(filter);

        return true;
    }

    public void postVisitEdge(Collection nodes, Object object, Object parameter) throws Exception {
        stack.pop();
    }

    public EntryDefinition getEntryDefinition() {
        return entryDefinition;
    }

    public void setEntryDefinition(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }

    public Stack getStack() {
        return stack;
    }

    public void setStack(Stack stack) {
        this.stack = stack;
    }

    public Set getKeys() {
        return keys;
    }

    public void setKeys(Set keys) {
        this.keys = keys;
    }

    public Source getPrimarySource() {
        return primarySource;
    }

    public void setPrimarySource(Source primarySource) {
        this.primarySource = primarySource;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
}
