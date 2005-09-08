/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.sync.SyncService;
import org.safehaus.penrose.graph.GraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    public EngineContext engineContext;
    public EntryDefinition entryDefinition;

    private int returnCode = LDAPException.SUCCESS;

    private Stack stack = new Stack();

    public AddGraphVisitor(
            EngineContext engineContext,
            EntryDefinition entryDefinition,
            AttributeValues sourceValues
            ) throws Exception {

        this.engineContext = engineContext;
        this.entryDefinition = entryDefinition;

        stack.push(sourceValues);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        AttributeValues sourceValues = (AttributeValues)stack.peek();

        log.debug("Adding "+source.getName()+" with: "+sourceValues);

        if (!source.isIncludeOnAdd()) {
            log.debug("Source "+source.getName()+" is not included on add");
            return true;
        }

        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            return true;
        }

        AttributeValues newSourceValues = new AttributeValues();
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);

            if (!name.startsWith(source.getName()+".")) continue;

            name = name.substring(source.getName().length()+1);
            newSourceValues.set(name, values);
        }

        returnCode = engineContext.getSyncService().add(source, newSourceValues);

        if (returnCode == LDAPException.NO_SUCH_OBJECT) return true; // ignore
        if (returnCode != LDAPException.SUCCESS) return false;

        return true;
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)edge;

        log.debug("Relationship "+relationship);
        if (entryDefinition.getSource(source.getName()) == null) return false;

        AttributeValues sourceValues = (AttributeValues)stack.peek();

        String lhs = relationship.getLhs();
        String rhs = relationship.getRhs();

        if (lhs.startsWith(source.getName()+".")) {
            String exp = lhs;
            lhs = rhs;
            rhs = exp;
        }

        Collection lhsValues = sourceValues.get(lhs);
        log.debug(" - "+lhs+" -> "+rhs+": "+lhsValues);

        AttributeValues newSourceValues = new AttributeValues();
        newSourceValues.add(sourceValues);
        newSourceValues.set(rhs, lhsValues);

        stack.push(newSourceValues);

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
