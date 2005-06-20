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
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
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
        Filter filter = createFilter(pks);
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

    public Filter createFilter(Collection keys) {

        Filter filter = null;

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            Filter f = createFilter(pk);

            if (filter == null) {
                filter = f;

            } else if (!(filter instanceof OrFilter)) {
                OrFilter of = new OrFilter();
                of.addFilterList(filter);
                of.addFilterList(f);
                filter = of;

            } else {
                OrFilter of = (OrFilter)filter;
                of.addFilterList(f);
            }
        }

        return filter;
    }

    public Filter createFilter(Row values) {

        Filter f = null;

        for (Iterator j=values.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Object value = values.get(name);
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter(name, "=", value == null ? null : value.toString());

            if (f == null) {
                f = sf;

            } else if (!(f instanceof AndFilter)) {
                AndFilter af = new AndFilter();
                af.addFilterList(f);
                af.addFilterList(sf);
                f = af;

            } else {
                AndFilter af = (AndFilter)f;
                af.addFilterList(sf);
            }
        }

        return f;
    }
}
