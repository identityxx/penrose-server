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

    private Handler handler;

    public DeleteHandler(Handler handler) throws Exception {
        this.handler = handler;
    }

    public int delete(PenroseSession session, String dn) throws Exception {

        log.info("-------------------------------------------------");
        log.info("DELETE:");
        if (session.getBindDn() != null) log.info(" - Bind DN: "+session.getBindDn());
        log.info(" - DN: "+dn);
        log.info("");

        DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, session, dn);
        handler.postEvent(dn, beforeDeleteEvent);

        int rc = performDelete(session, dn);

        DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, session, dn);
        afterDeleteEvent.setReturnCode(rc);
        handler.postEvent(dn, afterDeleteEvent);

        return rc;
    }

    public int performDelete(PenroseSession session, String dn) throws Exception {

        dn = LDAPDN.normalize(dn);

        Entry entry = getHandler().getSearchHandler().find(session, dn);
        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handler.getACLEngine().checkDelete(session, entry);
        if (rc != LDAPException.SUCCESS) return rc;

        log.debug("Deleting entry "+dn);

        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = handler.getPartitionManager().getPartition(entryMapping);

        if (partition.isDynamic(entryMapping)) {
	        return handler.getEngine().delete(entry);

        } else {
            return deleteStaticEntry(entryMapping);

        }
    }

    public int deleteStaticEntry(EntryMapping entry) throws Exception {

        Partition partition = handler.getPartitionManager().getPartitionByDn(entry.getDn());
        if (partition == null) return LDAPException.NO_SUCH_OBJECT;

        // can't delete no leaf
        Collection children = partition.getChildren(entry);
        if (children != null && !children.isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        partition.removeEntryMapping(entry);

        return LDAPException.SUCCESS;
    }

    public Handler getHandler() {
        return handler;
    }

    public void getHandler(Handler handler) {
        this.handler = handler;
    }
}
