/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine.impl;

import org.ietf.ldap.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.engine.ModifyHandler;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.ObjectClass;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * @author Endi S. Dewata
 */
public class DefaultModifyHandler extends ModifyHandler {

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldValues = entry.getAttributeValues();

        Date date = new Date();

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        ModifyGraphVisitor visitor = new ModifyGraphVisitor((DefaultEngine)getEngine(), this, entry, newValues, date);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();
/*
        Collection sources = entryDefinition.getSources();

        for (Iterator iterator = sources.iterator(); iterator.hasNext();) {
            Source source = (Source) iterator.next();

            int result = modify(source, entryDefinition, oldValues, newValues, date);

            if (result != LDAPException.SUCCESS) return result;
        }
*/
        getEngine().getEntryCache().remove(entryDefinition, oldValues, date);
        //engine.getEntryCache().insert(entryDefinition, newValues, date);

        return LDAPException.SUCCESS;
    }

    /**
     * Modify virtual entry in the LDAP tree
     *
     * @param entry
     * @param source
     * @param oldValues
     * @param newValues
     * @return return code
     * @throws Exception
     */
    public int modify(Source source, EntryDefinition entry, AttributeValues oldValues, AttributeValues newValues, Date date) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Updating entry in " + source.getName());
        //log.debug("Old values: " + oldValues);
        //log.debug("New values: " + newValues);

        MRSWLock lock = ((DefaultEngine)getEngine()).getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            Map oldEntries = getEngineContext().getTransformEngine().transform(source, oldValues);
            Map newEntries = getEngineContext().getTransformEngine().transform(source, newValues);

            //log.debug("Old entries: " + oldEntries);
            //log.debug("New entries: " + newEntries);

            Collection oldPKs = oldEntries.keySet();
            Collection newPKs = newEntries.keySet();

            log.debug("Old PKs: " + oldPKs);
            log.debug("New PKs: " + newPKs);

            Set addRows = new HashSet(newPKs);
            addRows.removeAll(oldPKs);
            log.debug("PKs to add: " + addRows);

            Set removeRows = new HashSet(oldPKs);
            removeRows.removeAll(newPKs);
            log.debug("PKs to remove: " + removeRows);

            Set replaceRows = new HashSet(oldPKs);
            replaceRows.retainAll(newPKs);
            log.debug("PKs to replace: " + replaceRows);

            // Add rows
            for (Iterator i = addRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues newEntry = (AttributeValues) newEntries.get(pk);
                log.debug("ADDING ROW: " + newEntry);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(newEntry);

                // Add row to source table in the source database/directory
                int rc = source.add(newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Add row to source table in the cache
                //engine.getSourceCache().insert(source, newEntry, date);
            }

            // Remove rows
            for (Iterator i = removeRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues) oldEntries.get(pk);
                log.debug("DELETE ROW: " + oldEntry);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(oldEntry);

                // Delete row from source table in the source database/directory
                int rc = source.delete(oldEntry);
                if (rc != LDAPException.SUCCESS)
                    return rc;

                // Delete row from source table in the cache
                getEngine().getSourceCache().delete(source, oldEntry, date);
            }

            // Replace rows
            for (Iterator i = replaceRows.iterator(); i.hasNext();) {
                Row pk = (Row) i.next();
                AttributeValues oldEntry = (AttributeValues) oldEntries.get(pk);
                AttributeValues newEntry = (AttributeValues) newEntries.get(pk);
                log.debug("REPLACE ROW: " + newEntry.toString());

                //AttributeValues oldAttributes = engineContext.getTransformEngine().convert(oldEntry);
                //AttributeValues newAttributes = engineContext.getTransformEngine().convert(newEntry);

                // Modify row from source table in the source database/directory
                int rc = source.modify(oldEntry, newEntry);
                if (rc != LDAPException.SUCCESS) return rc;

                // Modify row from source table in the cache
                getEngine().getSourceCache().delete(source, oldEntry, date);
                //engine.getSourceCache().insert(source, newEntry, date);
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

}