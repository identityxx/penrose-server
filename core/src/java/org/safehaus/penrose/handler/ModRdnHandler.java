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
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class ModRdnHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

    public ModRdnHandler(Handler handler) {
        this.handler = handler;
    }

    public int modrdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        int rc;
        try {

            rc = performModRdn(session, partition, entry, newRdn, deleteOldRdn);
            if (rc != LDAPException.SUCCESS) return rc;

            // refreshing entry cache

            String parentDn = EntryUtil.getParentDn(entry.getDn());
            String newDn = newRdn+","+parentDn;

            PenroseSession adminSession = handler.getPenrose().newSession();
            adminSession.setBindDn(handler.getPenrose().getPenroseConfig().getRootDn());

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_SUB);

            adminSession.search(
                    newDn,
                    "(objectClass=*)",
                    sc,
                    results
            );

            while (results.hasNext()) results.next();

            EntryMapping entryMapping = entry.getEntryMapping();
            handler.getEntryCache().remove(partition, entryMapping, entry.getDn());

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
        }

        if (rc == LDAPException.SUCCESS) {
            log.warn("ModRDN operation succeded.");
        } else {
            log.warn("ModRDN operation failed. RC="+rc);
        }

        return rc;
    }

    public int performModRdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            String newRdn,
            boolean deleteOldRdn)
            throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();

        if (partition.isProxy(entryMapping)) {
            return handler.getEngine("PROXY").modrdn(session, partition, entry, newRdn, deleteOldRdn);

        } else if (partition.isDynamic(entryMapping)) {
            return handler.getEngine().modrdn(session, partition, entry, newRdn, deleteOldRdn);

        } else {
            return modRdnStaticEntry(partition, entry, newRdn, deleteOldRdn);
        }
    }

    public int modRdnStaticEntry(
            Partition partition,
            Entry entry,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        partition.renameEntryMapping(entryMapping, newRdn);

        return LDAPException.SUCCESS;
    }
}
