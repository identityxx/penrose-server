/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.cache.SourceCache;
import org.safehaus.penrose.graph.GraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JoinGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    public EntryDefinition entryDefinition;
    public Source primarySource;
    public SourceCache sourceCache;

    private List fieldNames = new ArrayList();
    private List tableNames = new ArrayList();
    private List joins = new ArrayList();

    private Stack stack = new Stack();
    private AttributeValues attributeValues = new AttributeValues();

    public JoinGraphVisitor(EntryDefinition entryDefinition, Source primarySource, SourceCache sourceCache, Row pk) {
        this.entryDefinition = entryDefinition;
        this.primarySource = primarySource;
        this.sourceCache = sourceCache;

        Set pks = new HashSet();
        pks.add(pk);

        stack.push(pks);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        //log.debug("Source "+source);

        if (entryDefinition.getSource(source.getName()) == null) return false;

        Collection pks = (Collection)stack.peek();

        if (source.equals(primarySource)) {
            log.debug("Starting with source "+source+" with pks: "+pks);

        } else {
            log.debug("Joining with source "+source+" with pks: "+pks);
            pks = sourceCache.getByPks(source, pks).keySet();
        }

        Map results = sourceCache.get(source, pks);

        log.debug("Records:");
        for (Iterator i = results.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            log.debug(" - "+pk);

            AttributeValues values = (AttributeValues)results.get(pk);
            AttributeValues newValues = new AttributeValues();

            Map v = values.getValues();
            for (Iterator j = v.keySet().iterator(); j.hasNext(); ) {
                String name = (String)j.next();

                Collection c = (Collection)v.get(name);
                newValues.add(source.getName()+"."+name, c);
            }

            //log.debug(" - "+pk+": "+newValues);
            pks.add(pk);

            attributeValues.add(newValues);
        }

        stack.push(pks);
/*
        Collection fields = source.getFields();
        Set set = new HashSet();
        for (Iterator j = fields.iterator(); j.hasNext();) {
            Field field = (Field)j.next();
            String name = source.getName() + "." + field.getName();

            if (set.contains(name)) continue;
            set.add(name);

            getFieldNames().add(name);
        }

        //log.debug("Field names: "+getFieldNames());

        getTableNames().add(source.getSourceName()+" "+source.getName());

        //log.debug("Table names: "+getTableNames());
*/
        return true;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        stack.pop();
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)edge;

        if (entryDefinition.getSource(source.getName()) == null) return false;

        // visit this node if the pk fields are set
        boolean visit = true;

        Collection pkFields = source.getPrimaryKeyFields();
        for (Iterator i=pkFields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            visit &= field.getExpression() != null;
        }

        if (!visit) return false;

        log.debug("Relationship "+relationship);
/*
        getJoins().add(relationship.toString());
*/
        //log.debug("Joins: "+getJoins());

        String lhs = relationship.getLhs();
        String rhs = relationship.getRhs();

        if (lhs.startsWith(source.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        int li = lhs.indexOf(".");
        String lField = lhs.substring(li+1);

        int ri = rhs.indexOf(".");
        String rField = rhs.substring(ri+1);

        Collection pks = (Collection)stack.peek();

        Collection newPks = new HashSet();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            Object value = row.get(lField);
            Row newRow = new Row();
            newRow.set(rField, value);

            newPks.add(newRow);

            //log.debug(lField+" = "+rField+" => "+value);
        }
        //log.debug("New pks: "+newPks);

        stack.push(newPks);

        return true;
    }

    public void postVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        stack.pop();
    }

    public List getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List fieldNames) {
        this.fieldNames = fieldNames;
    }

    public List getTableNames() {
        return tableNames;
    }

    public void setTableNames(List tableNames) {
        this.tableNames = tableNames;
    }

    public List getJoins() {
        return joins;
    }

    public void setJoins(List joins) {
        this.joins = joins;
    }

    public AttributeValues getAttributeValues() {
        return attributeValues;
    }

    public void setAttributeValues(AttributeValues attributeValues) {
        this.attributeValues = attributeValues;
    }
}
