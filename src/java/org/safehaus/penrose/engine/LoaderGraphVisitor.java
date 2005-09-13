/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.GraphVisitor;
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
            log.debug(" - "+row);
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
                //log.debug("   checking "+values+" with "+filter);

                if (!engineContext.getSchema().partialMatch(newValues, filter)) continue;

                filterList = (Collection)filterMap.get(filter);
                break;
            }

            //log.debug("   original filter: "+mainPk);

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
                log.debug("   - "+av);
            }

        }

        stack.push(newFilterMap);

        return true;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        //stack.pop();
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
}
