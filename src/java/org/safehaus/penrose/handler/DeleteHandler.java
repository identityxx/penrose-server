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

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.event.DeleteEvent;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Entry;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPException;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler {

    Logger log = Logger.getLogger(getClass());

    private SessionHandler sessionHandler;

    public DeleteHandler(SessionHandler sessionHandler) throws Exception {
        this.sessionHandler = sessionHandler;
    }

    public int delete(PenroseSession session, String dn) throws Exception {

        log.info("-------------------------------------------------");
        log.info("DELETE:");
        if (session != null && session.getBindDn() != null) log.info(" - Bind DN: "+session.getBindDn());
        log.info(" - DN: "+dn);
        log.info("");

        DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, session, dn);
        sessionHandler.postEvent(dn, beforeDeleteEvent);

        String ndn = LDAPDN.normalize(dn);

        Entry entry = getHandler().getSearchHandler().find(session, ndn);
        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = performDelete(session, entry);

        sessionHandler.getEngine().getEntryCache().remove(entry);

        DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, session, dn);
        afterDeleteEvent.setReturnCode(rc);
        sessionHandler.postEvent(dn, afterDeleteEvent);

        return rc;
    }

    public int performDelete(PenroseSession session, Entry entry) throws Exception {

        int rc = sessionHandler.getACLEngine().checkDelete(session, entry);
        if (rc != LDAPException.SUCCESS) return rc;

        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = sessionHandler.getPartitionManager().getPartition(entryMapping);

        if (partition.isDynamic(entryMapping)) {
	        return sessionHandler.getEngine().delete(entry);

        } else {
            return deleteStaticEntry(entryMapping);

        }
    }

    public int deleteStaticEntry(EntryMapping entryMapping) throws Exception {

        log.debug("Deleting static entry "+entryMapping.getDn());

        Partition partition = sessionHandler.getPartitionManager().getPartitionByDn(entryMapping.getDn());
        if (partition == null) return LDAPException.NO_SUCH_OBJECT;

        // can't delete no leaf
        Collection children = partition.getChildren(entryMapping);
        if (!children.isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        partition.removeEntryMapping(entryMapping);

        return LDAPException.SUCCESS;
    }

    public SessionHandler getHandler() {
        return sessionHandler;
    }

    public void getHandler(SessionHandler sessionHandler) {
        this.sessionHandler = sessionHandler;
    }
}
