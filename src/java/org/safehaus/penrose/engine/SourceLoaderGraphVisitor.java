/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Relationship;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.SearchResults;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceLoaderGraphVisitor extends GraphVisitor {

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
        System.out.println("Source "+source.getName());

        if (entryDefinition.getSource(source.getName()) == null) return false;

        Collection pks = (Collection)stack.peek();
        Filter filter = engine.getEngineContext().getFilterTool().createFilter(pks);
        System.out.println("Loading source "+source.getName()+" with filter "+filter);

        SearchResults results = engine.getSourceCache().loadSource(entryDefinition, source, filter, date);

        Collection newRows = new HashSet();
        for (Iterator i = results.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            System.out.println(" - "+row);
            newRows.add(row);
        }

        stack.pop();
        stack.push(newRows);

        return true;
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)edge;

        System.out.println("Relationship "+relationship);
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
        System.out.println("Rows: "+rows);

        Collection newRows = new HashSet();
        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            Object value = row.get(lField);
            Row newRow = new Row();
            newRow.set(rField, value);

            newRows.add(newRow);

            //System.out.println(lField+" = "+rField+" => "+value);
        }
        System.out.println("New Rows: "+newRows);

        stack.push(newRows);

        return true;
    }

    public void postVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        stack.pop();
    }
}
