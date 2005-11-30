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
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;

/**
 * @author Endi S. Dewata
 */
public class ModRdnHandler {

    Logger log = Logger.getLogger(getClass());

    public Handler handler;

	public ModRdnHandler(Handler handler) throws Exception {
        this.handler = handler;
	}

	public int modrdn(PenroseSession session, String dn, String newRdn)
			throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("MODRDN:");
        if (session.getBindDn() != null) log.info(" - Bind DN: " + session.getBindDn());
        log.debug(" - DN: " + dn);
        log.debug(" - New RDN: " + newRdn);

        //return LDAPException.LDAP_NOT_SUPPORTED;

        int rc = performModRdn(session, dn, newRdn);

        return rc;
	}

    public int performModRdn(
            PenroseSession session,
            String dn,
            String newRdn)
			throws Exception {

		String ndn = LDAPDN.normalize(dn);

        Entry entry = handler.getSearchHandler().find(session, ndn);
        if (entry == null) return LDAPException.NO_SUCH_OBJECT;

        int rc = handler.getACLEngine().checkModify(session, entry);
        if (rc != LDAPException.SUCCESS) return rc;

        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = handler.getPartitionManager().getPartition(entryMapping);
        if (partition.isDynamic(entryMapping)) {
            return modRdnVirtualEntry(session, entry, newRdn);

        } else {
            return modRdnStaticEntry(entryMapping, newRdn);
        }
	}

    public int modRdnStaticEntry(
            EntryMapping entry,
            String newRdn)
			throws Exception {

        Partition partition = handler.getPartitionManager().getPartitionByDn(entry.getDn());
        partition.renameEntryMapping(entry, newRdn);

        return LDAPException.SUCCESS;
    }

    public int modRdnVirtualEntry(
            PenroseSession session,
            Entry entry,
			String newRdn)
            throws Exception {

        return handler.getEngine().modrdn(entry, newRdn);
    }
}
