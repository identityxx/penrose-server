/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine.impl;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.engine.DeleteHandler;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.thread.MRSWLock;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultDeleteHandler extends DeleteHandler {

    public int delete(EntryDefinition entryDefinition, AttributeValues values) throws Exception {

        Date date = new Date();

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        DeleteGraphVisitor visitor = new DeleteGraphVisitor(getEngine(), this, primarySource, entryDefinition, values, date);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();
/*
        Collection sources = entryDefinition.getSources();

        // delete from each source starting from the last one
        Source list[] = (Source[])sources.toArray(new Source[sources.size()]);

        for (int index=list.length-1; index>=0; index--) {
            Source source = list[index];

            int rc = delete(source, entryDefinition, values, date);

            if (rc == LDAPException.NO_SUCH_OBJECT) continue; // ignore
            if (rc != LDAPException.SUCCESS) return rc;
        }
*/
        getEngineContext().getCache().getFilterCache().invalidate();

        Entry entry = new Entry(entryDefinition, values);
        getEngine().getEntryCache().remove(entry);

        return LDAPException.SUCCESS;
    }

    /**
     * Delete virtual entry in the LDAP tree
     *
     * @param entry
     * @param source
     * @param values
     * @return return code
     * @throws Exception
     */
    public int delete(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

        log.info("-------------------------------------------------");
        log.debug("Deleting entry in "+source.getName());

        MRSWLock lock = getEngine().getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

	        log.debug("Values: "+values);

	        Map rows = getEngineContext().getTransformEngine().transform(source, values);

	        log.debug("Entries: "+rows);

	        log.debug("Rows to be deleted from "+source.getName()+": "+rows.size()+" rows");

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);

	            int rc = source.delete(attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            getEngine().getSourceCache().remove(source, pk);
	        }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }
}
