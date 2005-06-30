/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine.impl;

import org.ietf.ldap.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.engine.AddHandler;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.thread.MRSWLock;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultAddHandler extends AddHandler {

    public int add(
            PenroseConnection connection,
            EntryDefinition entryDefinition,
            AttributeValues values)
            throws Exception {

        Date date = new Date();

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        AddGraphVisitor visitor = new AddGraphVisitor((DefaultEngine)getEngine(), this, entryDefinition, values, date);
        graph.traverse(visitor, primarySource);

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();
/*
        Collection sources = entryDefinition.getSources();

        for (Iterator i2 = sources.iterator(); i2.hasNext(); ) {
            Source source = (Source)i2.next();

            int rc = add(source, entryDefinition, values, date);
            if (rc != LDAPException.SUCCESS) return rc;
        }

        engine.getEntryCache().put(entryDefinition, values, date);
*/
        return LDAPException.SUCCESS;
    }

    public int add(Source source, EntryDefinition entry, AttributeValues values, Date date) throws Exception {

        log.debug("----------------------------------------------------------------");
        log.debug("Adding entry into "+source.getName());
        log.debug("Values: "+values);

        MRSWLock lock = ((DefaultEngine)getEngine()).getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

	        Map rows = getEngineContext().getTransformEngine().transform(source, values);

	        log.debug("New entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);

	            // Add row to the source table in the source database/directory
	            int rc = source.add(attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            // Add row to the source table in the cache
	            getEngine().getSourceCache().insert(source, attributes, date);
	        }

        } finally {
        	lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

}
