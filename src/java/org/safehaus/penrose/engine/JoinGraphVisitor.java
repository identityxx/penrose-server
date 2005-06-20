/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.Field;
import org.safehaus.penrose.mapping.Relationship;
import org.safehaus.penrose.mapping.EntryDefinition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JoinGraphVisitor extends GraphVisitor {

    public EntryDefinition entryDefinition;

    private List fieldNames = new ArrayList();
    private List tableNames = new ArrayList();
    private List joins = new ArrayList();

    public JoinGraphVisitor(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        System.out.println("Source "+source);

        Collection fields = source.getFields();
        Set set = new HashSet();
        for (Iterator j = fields.iterator(); j.hasNext();) {
            Field field = (Field)j.next();
            String name = source.getName() + "." + field.getName();

            if (set.contains(name)) continue;
            set.add(name);

            getFieldNames().add(name);
        }

        System.out.println("Field names: "+getFieldNames());

        getTableNames().add(source.getSourceName()+" "+source.getName());

        System.out.println("Table names: "+getTableNames());

        return true;
    }

    public boolean preVisitEdge(Object node1, Object node2, Object object, Object parameter) throws Exception {
        Source source = (Source)node2;

        if (entryDefinition.getSource(source.getName()) == null) return false;

        Relationship relationship = (Relationship)object;
        System.out.println("Relationship "+relationship);

        getJoins().add(relationship.toString());

        System.out.println("Joins: "+getJoins());

        return true;
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
}
