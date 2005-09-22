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
import org.safehaus.penrose.interpreter.Interpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LoaderGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private EngineContext engineContext;
    private EntryDefinition entryDefinition;

    private Map sourceValues = new TreeMap();
    private Map attributeValues = new TreeMap();

    private Stack stack = new Stack();

    public LoaderGraphVisitor(
            EngineContext engineContext,
            EntryDefinition entryDefinition,
            Collection filters) {

        this.engineContext = engineContext;
        this.entryDefinition = entryDefinition;

        Map map = new HashMap();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Row filter = (Row)i.next();
            Collection filterList = new ArrayList();
            filterList.add(filter);
            map.put(filter, filterList);
        }
        stack.push(map);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        Map filterMap = (Map)stack.peek();

        log.debug("Loading "+source.getName()+" for:");
        for (Iterator i=filterMap.keySet().iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            Collection filterList = (Collection)filterMap.get(row);
            log.debug(" - "+row+": "+filterList);
        }

        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            return true;
        }
/*
        boolean allPksDefined = true; // check if the source is a connecting source

        Collection fields = source.getPrimaryKeyFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            allPksDefined &= field.getExpression() != null;
        }

        // if connecting source, dont visit
        log.debug(source+" is a connecting source: "+!allPksDefined);
        if (!allPksDefined) return false;
*/        

        Collection filters = filterMap.keySet();
        Map map = engineContext.getSyncService().search(source, filters);
        if (map.size() == 0) return false;

        log.debug("Records:");
        Map newFilterMap = new HashMap();
        for (Iterator i = map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues values = (AttributeValues)map.get(pk);
            log.debug(" - "+pk+": "+values);

            Collection list = engineContext.getTransformEngine().convert(values);
            Collection newRows = new ArrayList();
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();
                Row newRow = new Row();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    if (value == null) continue;
                    newRow.set(source.getName()+"."+name, value);
                }
                newRows.add(newRow);
            }

            AttributeValues newValues = new AttributeValues();
            for (Iterator j=values.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection value = values.get(name);
                if (value == null) continue;
                newValues.set(source.getName()+"."+name, value);
            }

            // find the original filter that produces this record
            Collection filterList = null;

            for (Iterator j=filters.iterator(); j.hasNext(); ) {
                Row filter = (Row)j.next();

                Row newFilter = new Row();
                for (Iterator k=filter.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    if (!name.startsWith(source.getName()+".")) continue;
                    newFilter.set(name, filter.get(name));
                }

                //log.debug("   checking "+newValues+" with "+newFilter);

                if (!engineContext.getSchema().partialMatch(newValues, newFilter)) continue;

                filterList = (Collection)filterMap.get(filter);
                break;
            }

            //log.debug("   original filters: "+filterList);
            if (filterList == null) {
                filterList = new ArrayList();
                Row rdn = new Row();
                Interpreter interpreter = engineContext.newInterpreter();
                interpreter.set(newValues);

                for (Iterator j=entryDefinition.getAttributeDefinitions().iterator(); j.hasNext(); ) {
                    AttributeDefinition ad = (AttributeDefinition)j.next();
                    if (!ad.isRdn()) continue;

                    Expression expression = ad.getExpression();
                    Object value = interpreter.eval(expression.getScript());
                    if (value == null) continue;

                    rdn.set(ad.getName(), value);
                }
                filterList.add(rdn);
            }

            for (Iterator j=newRows.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();
                newFilterMap.put(row, filterList);
            }

            for (Iterator j=filterList.iterator(); j.hasNext(); ) {
                Row filter = (Row)j.next();

                AttributeValues av = (AttributeValues)attributeValues.get(filter);
                if (av == null) {
                    av = new AttributeValues();
                    attributeValues.put(filter, av);
                }

                av.add(newValues);
                //log.debug("   - "+av);
            }

        }

        stack.push(newFilterMap);

        return true;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        stack.pop();
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)edge;

        log.debug("Relationship "+relationship);
        Map map = (Map)stack.peek();
        Collection rows = map.keySet();

        if (entryDefinition.getSource(source.getName()) == null) return false;
/*
        boolean allPksDefined = true; // check if the source is a connecting source

        Collection fields = source.getPrimaryKeyFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            allPksDefined &= field.getExpression() != null;
        }

        // if connecting source, dont visit
        if (!allPksDefined) return false;
*/

        String lhs = relationship.getLhs();
        String rhs = relationship.getRhs();

        if (lhs.startsWith(source.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        Map newMap = new HashMap();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            Collection filterList = (Collection)map.get(row);
            log.debug(" - "+row+": "+filterList);

            Object value = row.get(lhs);
            if (value == null) continue;

            Row newRow = new Row();
            newRow.set(rhs, value);
            //newRow.add(row);

            Collection list = (Collection)newMap.get(newRow);
            if (list == null) {
                list = new ArrayList();
                newMap.put(newRow, list);
            }
            list.addAll(filterList);

            log.debug("   - "+lhs+" = "+rhs+" => "+value);
        }
        //log.debug("New Rows: "+newMap);

        stack.push(newMap);

        return true;
    }

    public void postVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        stack.pop();
    }

    public EntryDefinition getEntryDefinition() {
        return entryDefinition;
    }

    public void setEntryDefinition(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }

    public Map getAttributeValues() {
        return attributeValues;
    }

    public void setAttributeValues(Map attributeValues) {
        this.attributeValues = attributeValues;
    }

    public Map getSourceValues() {
        return sourceValues;
    }

    public void setSourceValues(Map sourceValues) {
        this.sourceValues = sourceValues;
    }
}
