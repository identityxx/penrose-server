/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.AttributeValues;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public abstract class DeleteHandler {

    public Logger log = Logger.getLogger(Penrose.DELETE_LOGGER);

    private Engine engine;
    private EngineContext engineContext;

    public void init(Engine engine) throws Exception {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();

        init();
    }

    public void init() throws Exception {        
    }

    public int delete(PenroseConnection connection, String dn) throws Exception {

        String ndn = LDAPDN.normalize(dn);

        int result;

        EntryDefinition entryDefinition = getEngine().getConfig().getEntryDefinition(ndn);
        if (entryDefinition != null) {

        	// Static Entry
        	result = deleteStaticEntry(entryDefinition);

        } else {

        	// Virtual Entry
	        Entry entry = null;
	        try {
                entry = engine.getSearchHandler().find(connection, ndn);
	        } catch (Exception e) {
	            // ignore
	        }

	        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

            entryDefinition = entry.getEntryDefinition();
            AttributeValues values = entry.getAttributeValues();

	        result = delete(entryDefinition, values);

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

        getEngine().getConfig().removeEntryDefinition(entry);

        return LDAPException.SUCCESS;
    }

    public abstract int delete(EntryDefinition entryDefinition, AttributeValues values) throws Exception;

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
    }
}
