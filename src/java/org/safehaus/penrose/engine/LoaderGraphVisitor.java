/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.sync.SyncService;
import org.safehaus.penrose.graph.GraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LoaderGraphVisitor extends GraphVisitor {

    Logger log = LoggerFactory.getLogger(getClass());

    private SyncService syncService;
    private EngineContext engineContext;
    private EntryDefinition entryDefinition;
    private Map attributeValues = new HashMap();

    private Stack stack = new Stack();

    public LoaderGraphVisitor(EngineContext engineContext, SyncService syncService, EntryDefinition entryDefinition, Collection pks) {
        this.engineContext = engineContext;
        this.syncService = syncService;
        this.entryDefinition = entryDefinition;

        Map map = new TreeMap();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            map.put(pk, pk);
        }
        stack.push(map);
    }

    public boolean preVisitNode(Object node, Object parameter) throws Exception {
        Source source = (Source)node;
        //log.debug("Source "+source.getName());

        if (entryDefinition.getSource(source.getName()) == null) return false;
/*
        boolean allPksDefined = true; // check if the source is a connecting source

        Collection fields = source.getPrimaryKeyFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            allPksDefined &= field.getExpression() != null;
        }

        // if connecting source, dont visit
        log.debug(source+" is a connecting source: "+!allPksDefined);
        if (!allPksDefined) return false;
*/        
        Map map = (Map)stack.peek();
        log.debug("MAP: "+map);

        Collection pks = map.keySet();

        Map results = syncService.search(source, pks);
        if (results.size() == 0) return false;
        
        log.debug("Records:");
        Map newMap = new HashMap();
        for (Iterator i = results.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            log.debug(" - "+pk);

            AttributeValues values = (AttributeValues)results.get(pk);
            AttributeValues newValues = new AttributeValues();

            Map v = values.getValues();
            for (Iterator j = v.keySet().iterator(); j.hasNext(); ) {
                String name = (String)j.next();

                Collection c = (Collection)v.get(name);
                if (c == null) continue;
                
                newValues.add(source.getName()+"."+name, c);
            }

            Row mainPk = null;
            for (Iterator j=pks.iterator(); j.hasNext(); ) {
                Row p = (Row)j.next();
                //log.debug("   checking "+values+" with "+p);

                if (!engineContext.getSchema().partialMatch(values, p)) continue;

                mainPk = (Row)map.get(p);
                break;
            }

            //log.debug("   original filter: "+mainPk);

            newMap.put(pk, mainPk);

            AttributeValues av = (AttributeValues)attributeValues.get(mainPk);
            if (av == null) {
                av = new AttributeValues();
                attributeValues.put(mainPk, av);
            }

            av.add(newValues);

            //log.debug("   - "+av);
        }

        stack.push(newMap);

        return true;
    }

    public void postVisitNode(Object node, Object parameter) throws Exception {
        //stack.pop();
    }

    public boolean preVisitEdge(Object node1, Object node2, Object edge, Object parameter) throws Exception {
        Source source = (Source)node2;
        Relationship relationship = (Relationship)edge;

        if (entryDefinition.getSource(source.getName()) == null) return false;

        boolean allPksDefined = true; // check if the source is a connecting source

        Collection fields = source.getPrimaryKeyFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            allPksDefined &= field.getExpression() != null;
        }

        // if connecting source, dont visit
        if (!allPksDefined) return false;

        log.debug("Relationship "+relationship);

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

        Map map = (Map)stack.peek();
        Collection pks = map.keySet();
        //log.debug("Rows: "+pks);

        Map newMap = new HashMap();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            Row mainPk = (Row)map.get(pk);

            Object value = pk.get(lField);
            Row newPk = new Row();
            newPk.set(rField, value);

            newMap.put(newPk, mainPk);

            //log.debug(lField+" = "+rField+" => "+value);
        }
        //log.debug("New Rows: "+newMap);

        stack.push(newMap);

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

    public Map getAttributeValues() {
        return attributeValues;
    }

    public void setAttributeValues(Map attributeValues) {
        this.attributeValues = attributeValues;
    }
}
