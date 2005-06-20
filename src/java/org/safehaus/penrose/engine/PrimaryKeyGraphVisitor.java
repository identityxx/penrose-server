/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PrimaryKeyGraphVisitor extends GraphVisitor {

    private Engine engine;
    private EntryDefinition entryDefinition;
    private Stack stack = new Stack();
    private Set keys = new HashSet();

    public PrimaryKeyGraphVisitor(Engine engine, EntryDefinition entryDefinition, Row row) {
        this.engine = engine;
        this.entryDefinition = entryDefinition;

        Set rows = new HashSet();
        if (row != null) rows.add(row);
        stack.push(rows);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        System.out.println("Source "+source.getName());

        Collection rows = (Collection)stack.peek();

        Collection newRows = new HashSet();
        stack.push(newRows);

        if (entryDefinition.getSource(source.getName()) == null) return true;

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            Interpreter interpreter = engine.getEngineContext().newInterpreter();
            // interpreter.set(row);

            for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                Object value = row.get(name);
                interpreter.set(source.getName()+"."+name, value);
            }

            Collection rdnAttributes = entryDefinition.getRdnAttributes();

            Row pk = new Row();
            boolean valid = true;

            for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                AttributeDefinition attr = (AttributeDefinition)k.next();
                String name = attr.getName();
                String expression = attr.getExpression();
                Object value = interpreter.eval(expression);

                if (value == null) {
                    valid = false;
                    break;
                }

                pk.set(name, value);
            }

            if (!valid) continue;

            keys.add(pk);
            System.out.println(" - "+row+" => "+pk);
        }

        return false;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        stack.pop();
    }

    public boolean preVisitEdge(Object node1, Object node2, Object object, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)object;

        Collection newRows = (Collection)stack.pop();
        Collection rows = (Collection)stack.peek();

        String lhs = relationship.getLhs();
        String rhs = relationship.getRhs();

        if (lhs.startsWith(source.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        int index = rhs.indexOf(".");
        String fieldName = rhs.substring(index+1);

        System.out.println("Relationship "+lhs+" = "+rhs);

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            Object value = row.get(lhs);
            Row newRow = new Row();
            newRow.set(fieldName, value);

            newRows.add(newRow);

            System.out.println(rhs+" = "+value);
        }

        Filter newFilter = createFilter(newRows);

        System.out.println("Searching source "+source.getName()+" for "+newFilter);
        SearchResults results = source.search(newFilter);

        newRows = new HashSet();
        for (Iterator j=results.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();
            //System.out.println(" - "+row);
            newRows.add(row);
        }

        stack.push(newRows);

        return true;
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

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
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
}
