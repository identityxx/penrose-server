/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.graph.GraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private Source primarySource;

    private Stack stack = new Stack();
    private Set keys = new HashSet();

    public SearchGraphVisitor(
            EngineContext engineContext,
            EntryDefinition entryDefinition,
            Collection rows,
            Source primarySource) {

        this.engineContext = engineContext;
        this.entryDefinition = entryDefinition;
        this.primarySource = primarySource;

        stack.push(rows);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        Collection rows = (Collection)stack.peek();
        log.debug("Searching "+source.getName()+" for "+rows);

        //Collection newRows = new HashSet();
        //stack.push(newRows);

        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            //newRows.addAll(rows);
            return true;
        }

        Map map = engineContext.getSyncService().search(source, rows);
        if (map.size() == 0) return false;

        Collection results = new ArrayList();
        for (Iterator i=map.values().iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();
            Collection r = engineContext.getTransformEngine().convert(av);
            results.addAll(r);
        }

        stack.push(results);

        if (results != null) keys.addAll(results);

        if (source != primarySource) {
            log.debug("Source "+source.getName()+" is not the primary source of entry "+entryDefinition.getDn());
/*
            log.debug("Converting rows: "+rows);
            for (Iterator i=rows.iterator(); i.hasNext(); ) {
                Row row = (Row)i.next();

                Row newRow = new Row();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    newRow.set(source.getName()+"."+name, value);
                }

                newRows.add(newRow);
            }
            log.debug("to: "+newRows);
*/
            return true;
        }

        return false;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        //stack.pop();
    }

    public boolean preVisitEdge(Object node1, Object node2, Object object, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)object;

        //Collection newRows = (Collection)stack.pop();
        Collection rows = (Collection)stack.peek();

        String lhs = relationship.getLhs();
        String rhs = relationship.getRhs();

        if (lhs.startsWith(source.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        int li = lhs.indexOf(".");
        String lFieldName = lhs.substring(li+1);
        int ri = rhs.indexOf(".");
        String rFieldName = rhs.substring(ri+1);

        log.debug("Relationship "+lhs+" = "+rhs+":");

        Collection newRows = new HashSet();
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            Object value = row.get(lFieldName);
            if (value == null) continue;

            Row newRow = new Row();
            newRow.set(rFieldName, value);
            newRows.add(newRow);

            log.debug("   - "+lFieldName+" -> "+rFieldName+" = "+value);
        }

        if (newRows.size() == 0) return false;

        stack.push(newRows);

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
}
