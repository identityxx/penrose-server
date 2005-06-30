/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;

import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.Field;
import org.safehaus.penrose.mapping.Relationship;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.graph.GraphVisitor;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JoinGraphVisitor extends GraphVisitor {

    public Logger log = Logger.getLogger(Penrose.SEARCH_LOGGER);

    public EntryDefinition entryDefinition;

    private List fieldNames = new ArrayList();
    private List tableNames = new ArrayList();
    private List joins = new ArrayList();

    public JoinGraphVisitor(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        log.debug("Source "+source);

        Collection fields = source.getFields();
        Set set = new HashSet();
        for (Iterator j = fields.iterator(); j.hasNext();) {
            Field field = (Field)j.next();
            String name = source.getName() + "." + field.getName();

            if (set.contains(name)) continue;
            set.add(name);

            getFieldNames().add(name);
        }

        log.debug("Field names: "+getFieldNames());

        getTableNames().add(source.getSourceName()+" "+source.getName());

        log.debug("Table names: "+getTableNames());

        return true;
    }

    public boolean preVisitEdge(Object node1, Object node2, Object object, Object parameter) throws Exception {
        Source source = (Source)node2;

        if (entryDefinition.getSource(source.getName()) == null) return false;

        Relationship relationship = (Relationship)object;
        log.debug("Relationship "+relationship);

        getJoins().add(relationship.toString());

        log.debug("Joins: "+getJoins());

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
