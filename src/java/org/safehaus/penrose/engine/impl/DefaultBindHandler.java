/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine.impl;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.engine.BindHandler;
import org.safehaus.penrose.thread.MRSWLock;

import java.util.*;


/**
 * @author Endi S. Dewata
 */
public class DefaultBindHandler extends BindHandler {

    public int bind(EntryDefinition entry, AttributeValues values, String password) throws Exception {

        Date date = new Date();

        Collection sources = entry.getSources();

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            int rc = bind(source, entry, values, password, date);

            if (rc == LDAPException.SUCCESS) return rc;
        }

        return LDAPException.INVALID_CREDENTIALS;
    }

    public int bind(Source source, EntryDefinition entry, AttributeValues attributes, String password, Date date) throws Exception {
        log.debug("----------------------------------------------------------------");
        log.debug("Binding as entry in "+source.getName());
        log.debug("Values: "+attributes);

        MRSWLock lock = getEngine().getLock(source);
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        try {

	        Map rows = getEngineContext().getTransformEngine().transform(source, attributes);

	        log.debug("Entries: "+rows);

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Map pk = (Map)i.next();
	            AttributeValues row = (AttributeValues)rows.get(pk);

	            int rc = source.bind(row, password);

	            if (rc != LDAPException.SUCCESS) return rc;
	        }

        } finally {
        	lock.releaseReadLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }

}
