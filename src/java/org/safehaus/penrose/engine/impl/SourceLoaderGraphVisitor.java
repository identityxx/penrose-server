/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine.impl;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.graph.GraphVisitor;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceLoaderGraphVisitor extends GraphVisitor {

    public Logger log = Logger.getLogger(Penrose.SEARCH_LOGGER);

    public Engine engine;
    public EntryDefinition entryDefinition;
    public Date date;

    private Stack stack = new Stack();

    public SourceLoaderGraphVisitor(Engine engine, EntryDefinition entryDefinition, Collection pks, Date date) {
        this.engine = engine;
        this.entryDefinition = entryDefinition;
        this.date = date;

        stack.push(pks);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        //log.debug("Source "+source.getName());

        if (entryDefinition.getSource(source.getName()) == null) return false;

        boolean load = true;

        Collection fields = source.getPrimaryKeyFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            load &= field.getExpression() != null;
        }

        if (!load) return false;
        
        Collection pks = (Collection)stack.peek();

        SearchResults results = engine.getSourceCache().loadSource(entryDefinition, source, pks, date);
        if (results.size() == 0) return false;
        
        Collection newRows = new HashSet();
        for (Iterator i = results.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            //log.debug(" - "+row);
            newRows.add(row);
        }

        stack.pop();
        stack.push(newRows);

        return true;
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)edge;

        log.debug("Relationship "+relationship);
        if (entryDefinition.getSource(source.getName()) == null) return false;

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

        Collection rows = (Collection)stack.peek();
        //log.debug("Rows: "+rows);

        Collection newRows = new HashSet();
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            Object value = row.get(lField);
            Row newRow = new Row();
            newRow.set(rField, value);

            newRows.add(newRow);

            //log.debug(lField+" = "+rField+" => "+value);
        }
        //log.debug("New Rows: "+newRows);

        stack.push(newRows);

        return true;
    }

    public void postVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        stack.pop();
    }
}
