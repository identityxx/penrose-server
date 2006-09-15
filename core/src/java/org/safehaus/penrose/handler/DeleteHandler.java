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
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.util.ExceptionUtil;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class DeleteHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public DeleteHandler(Handler handler) {
        this.handler = handler;
    }

    public int delete(PenroseSession session, String dn) throws Exception {

        int rc;
        try {

            log.warn("Delete entry \""+dn+"\".");

            log.debug("-------------------------------------------------");
            log.debug("DELETE:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - DN: "+dn);
            log.debug("");

            Partition partition = handler.getPartitionManager().findPartition(dn);

            if (partition == null) {
                log.debug("Entry "+dn+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            Collection path = handler.getFindHandler().find(partition, dn);

            if (path == null || path.isEmpty()) {
                log.debug("Entry "+dn+" not found");
                return LDAPException.NO_SUCH_OBJECT;
            }

            Entry entry = (Entry)path.iterator().next();

            rc = performDelete(session, partition, entry);
            if (rc != LDAPException.SUCCESS) return rc;

            handler.getEngine().getEntryCache().remove(partition, entry);

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
        }

        if (rc == LDAPException.SUCCESS) {
            log.warn("Delete operation succeded.");
        } else {
            log.warn("Delete operation failed. RC="+rc);
        }

        return rc;
    }

    public int performDelete(PenroseSession session, Partition partition, Entry entry) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();

        int rc = handler.getACLEngine().checkDelete(session, entry.getDn(), entryMapping);
        if (rc != LDAPException.SUCCESS) {
            log.debug("Not allowed to delete "+entry.getDn());
            return rc;
        }

        if (partition.isProxy(entryMapping)) {
            return handler.getEngine("PROXY").delete(session, partition, entry);

        } else if (partition.isDynamic(entryMapping)) {
	        return handler.getEngine().delete(session, partition, entry);

        } else {
            return deleteStaticEntry(partition, entryMapping);
        }
    }

    public int deleteStaticEntry(Partition partition, EntryMapping entryMapping) throws Exception {

        log.debug("Deleting static entry "+entryMapping.getDn());

        // can't delete no leaf
        Collection children = partition.getChildren(entryMapping);
        if (!children.isEmpty()) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        partition.removeEntryMapping(entryMapping);

        return LDAPException.SUCCESS;
    }

    public Handler getHandler() {
        return handler;
    }

    public void getHandler(Handler handler) {
        this.handler = handler;
    }
}
