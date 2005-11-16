/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.event.DeleteEvent;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Entry;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler {

    Logger log = Logger.getLogger(getClass());

    private Handler handler;
    private HandlerContext handlerContext;

    public DeleteHandler(Handler handler) throws Exception {
        this.handler = handler;
        this.handlerContext = handler.getHandlerContext();
    }

    public int delete(PenroseConnection connection, String dn) throws Exception {

        log.info("-------------------------------------------------");
        log.info("DELETE:");
        if (connection.getBindDn() != null) log.info(" - Bind DN: "+connection.getBindDn());
        log.info(" - DN: "+dn);
        log.info("");

        DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, connection, dn);
        handler.postEvent(dn, beforeDeleteEvent);

        int rc = performDelete(connection, dn);

        DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, connection, dn);
        afterDeleteEvent.setReturnCode(rc);
        handler.postEvent(dn, afterDeleteEvent);

        return rc;
    }

    public int performDelete(PenroseConnection connection, String dn) throws Exception {

        dn = LDAPDN.normalize(dn);

        Entry entry = getHandler().getSearchHandler().find(connection, dn);
        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handlerContext.getACLEngine().checkDelete(connection, entry);
        if (rc != LDAPException.SUCCESS) return rc;

        log.debug("Deleting entry "+dn);

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        if (entryDefinition.isDynamic()) {
	        return handler.getEngine().delete(entry);

        } else {
            return deleteStaticEntry(entryDefinition);

        }
    }

    public int deleteStaticEntry(EntryDefinition entry) throws Exception {

        Config config = getHandlerContext().getConfig(entry.getDn());
        if (config == null) return LDAPException.NO_SUCH_OBJECT;

        // can't delete no leaf
        Collection children = config.getChildren(entry);
        if (children != null && !children.isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

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
