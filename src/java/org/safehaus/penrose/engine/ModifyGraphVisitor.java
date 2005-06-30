/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.interpreter.Interpreter;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyGraphVisitor extends GraphVisitor {

    public Logger log = Logger.getLogger(Penrose.MODIFY_LOGGER);

    public DefaultEngine engine;
    public DefaultModifyHandler modifyHandler;
    public EntryDefinition entryDefinition;
    public AttributeValues oldValues;
    public AttributeValues newValues;
    public Date date;
    private int returnCode = LDAPException.SUCCESS;

    private Stack stack = new Stack();

    public ModifyGraphVisitor(
            DefaultEngine engine,
            DefaultModifyHandler modifyHandler,
            Entry entry,
            AttributeValues newValues,
            Date date) throws Exception {

        this.engine = engine;
        this.modifyHandler = modifyHandler;
        this.entryDefinition = entry.getEntryDefinition();
        this.oldValues = entry.getAttributeValues();
        this.newValues = newValues;
        this.date = date;

        Collection rows = engine.getEngineContext().getTransformEngine().convert(oldValues);
        Collection keys = new HashSet();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            Interpreter interpreter = engine.getEngineContext().newInterpreter();
            interpreter.set(row);

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
        }

        log.debug("Primary keys: "+keys);
        stack.push(keys);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        //log.debug("Source "+source.getName());

        if (entryDefinition.getSource(source.getName()) == null) return false;

        returnCode = modifyHandler.modify(source, entryDefinition, oldValues, newValues, date);
        if (returnCode != LDAPException.SUCCESS) return false;

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

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }
}
