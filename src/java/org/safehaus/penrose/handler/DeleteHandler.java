/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.AttributeValues;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;
    private HandlerContext handlerContext;

    public DeleteHandler(Handler handler) throws Exception {
        this.handler = handler;
        this.handlerContext = handler.getHandlerContext();
    }

    public int delete(PenroseConnection connection, String dn) throws Exception {

        dn = LDAPDN.normalize(dn);

        // find existing entry
        Entry entry = null;
        try {
            entry = getHandler().getSearchHandler().find(connection, dn);
        } catch (Exception e) {
            // ignore
        }

        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handlerContext.getACLEngine().checkDelete(connection, entry);
        if (rc != LDAPException.SUCCESS) return rc;

        log.debug("Deleting entry "+dn);

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        if (entryDefinition.isDynamic()) {

            AttributeValues values = entry.getAttributeValues();

	        rc = handlerContext.getEngine().delete(entryDefinition, values);

            if (rc == LDAPException.SUCCESS) {
                String key = entryDefinition.getRdn()+","+entry.getParent().getDn();
                getHandlerContext().getCache().getEntryFilterCache().invalidate(key);
            }

        } else {

            // Static Entry
            rc = deleteStaticEntry(entryDefinition);

        }

        return rc;
    }

    public int deleteStaticEntry(EntryDefinition entry) throws Exception {

        // can't delete no leaf
        if (!entry.getChildren().isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        // detach from parent
        EntryDefinition parent = entry.getParent();
        if (parent != null) {
            Collection children = parent.getChildren();
            children.remove(entry);
        }

        Config config = getHandlerContext().getConfig(entry.getDn());
        if (config == null) return LDAPException.NO_SUCH_OBJECT;

        config.removeEntryDefinition(entry);

        return LDAPException.SUCCESS;
    }

    public Handler getHandler() {
        return handler;
    }

    public void getHandler(Handler handler) {
        this.handler = handler;
    }

    public HandlerContext getHandlerContext() {
        return handlerContext;
    }

    public void setHandlerContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }
}
