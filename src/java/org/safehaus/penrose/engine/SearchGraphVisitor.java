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
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.connection.Connection;
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
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private Source primarySource;

    private Stack stack = new Stack();
    private Set keys = new HashSet();

    public SearchGraphVisitor(
            Config config,
            Graph graph,
            EngineContext engineContext,
            EntryDefinition entryDefinition,
            Collection filters,
            Source primarySource) {

        this.config = config;
        this.graph = graph;
        this.engineContext = engineContext;
        this.entryDefinition = entryDefinition;
        this.primarySource = primarySource;

        stack.push(filters);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        Collection pks = (Collection)stack.peek();

        log.debug("Searching "+source.getName()+" for:");
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);
        }

        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            return true;
        }

        Filter filter = createFilter(source, pks);

        Collection relationships = graph.getEdgeObjects(source);

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            if (isJoinRelationship(relationship)) continue;

            Collection operands = relationship.getOperands();
            Iterator iterator = operands.iterator();
            String operand = (String)iterator.next();
            int index = operand.indexOf(".");
            String attribute = operand.substring(index+1);

            String value = (String)iterator.next();

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

        Map map = engineContext.getSyncService().search(source, filter);
        if (map.size() == 0) return false;

        Collection results = new ArrayList();
        for (Iterator i=map.values().iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();
            Collection list = engineContext.getTransformEngine().convert(av);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();
                Row newRow = new Row();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    if (value == null) continue;
                    newRow.set(source.getName()+"."+name, value);
                }
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
            String operand = (String)j.next();

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

    public Filter createFilter(Source source, Collection pks) throws Exception {
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection normalizedFilters = null;
        if (pks != null) {
            normalizedFilters = new TreeSet();
            for (Iterator i=pks.iterator(); i.hasNext(); ) {
                Row filter = (Row)i.next();

                Row f = new Row();
                for (Iterator j=filter.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    String newName = name;
                    if (name.startsWith(source.getName()+".")) newName = name.substring(source.getName().length()+1);

                    if (source.getField(newName) == null) continue;
                    f.set(newName, filter.get(name));
                }

                Row normalizedFilter = engineContext.getSchema().normalize(f);
                normalizedFilters.add(normalizedFilter);
            }
        }

        Filter filter = null;
        if (pks != null) {
            filter = engineContext.getFilterTool().createFilter(normalizedFilters);
        }

        return filter;
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
        String rhs = relationship.getRhs();

        if (lhs.startsWith(toSource.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        Collection newRows = new HashSet();
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            Object value = row.get(lhs);
            if (value == null) continue;

            Row newRow = new Row();
            newRow.set(rhs, value);
            newRows.add(newRow);

            log.debug("   - "+lhs+" -> "+rhs+" = "+value);
        }

        if (newRows.size() == 0) return false;

        stack.push(newRows);

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
