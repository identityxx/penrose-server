/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.thread.MRSWLock;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DefaultDeleteHandler implements DeleteHandler {

    public Logger log = Logger.getLogger(Penrose.DELETE_LOGGER);

    public DefaultEngine engine;
    public EngineContext engineContext;
    public Config config;

    public void init(Engine engine, EngineContext engineContext) throws Exception {
        this.engine = ((DefaultEngine)engine);
        this.engineContext = engineContext;
        this.config = engineContext.getConfig();
    }

    public int delete(PenroseConnection connection, String dn) throws Exception {

        String ndn = LDAPDN.normalize(dn);

        int result;

        EntryDefinition entry = config.getEntryDefinition(ndn);
        if (entry != null) {
        	
        	// Static Entry
        	result = deleteStaticEntry(entry);
        	
        } else {

        	// Virtual Entry
	        Entry sr = null;
	        try {
                sr = ((DefaultSearchHandler)engine.getSearchHandler()).getVirtualEntry(connection, ndn, new ArrayList());
	        } catch (Exception e) {
	            // ignore
	        }
	
	        if (sr == null) return LDAPException.NO_SUCH_OBJECT;

            entry = sr.getEntryDefinition();
            AttributeValues values = sr.getAttributeValues();

	        result = delete(entry, values);
	        
        }
        return result;
    }

    public int deleteStaticEntry(EntryDefinition entry) throws Exception {
        log.debug("Deleting static entry "+entry.getDn());

        // can't delete no leaf
        if (!entry.getChildren().isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        // detach from parent
        EntryDefinition parent = entry.getParent();
        if (parent != null) {
            Collection children = parent.getChildren();
            children.remove(entry);
        }

        config.removeEntryDefinition(entry);

        return LDAPException.SUCCESS;
    }


    public int delete(EntryDefinition entry, AttributeValues values) throws Exception {

        Date date = new Date();

        Collection sources = entry.getSources();

        // delete from each source starting from the last one
        Source list[] = (Source[])sources.toArray(new Source[sources.size()]);

        for (int index=list.length-1; index>=0; index--) {
            Source source = list[index];

            int rc = delete(source, entry, values, date);

            if (rc == LDAPException.NO_SUCH_OBJECT) continue; // ignore
            if (rc != LDAPException.SUCCESS) return rc;
        }

        engine.getEntryCache().delete(entry, values, date);

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

        MRSWLock lock = engine.getLock(source);
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

	        log.debug("Values: "+values);

	        Map rows = engineContext.getTransformEngine().transform(source, values);

	        log.debug("Entries: "+rows);

	        log.debug("Rows to be deleted from "+source.getName()+": "+rows.size()+" rows");

	        for (Iterator i=rows.keySet().iterator(); i.hasNext(); ) {
	            Row pk = (Row)i.next();
	            AttributeValues attributes = (AttributeValues)rows.get(pk);

                //AttributeValues attributes = engineContext.getTransformEngine().convert(row);

	            int rc = source.delete(attributes);
	            if (rc != LDAPException.SUCCESS) return rc;

	            engine.getSourceCache().delete(source, attributes, date);
	        }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return LDAPException.SUCCESS;
    }
}
