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
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPSearchConstraints;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModRdnHandler {

    Logger log = Logger.getLogger(getClass());

    public Handler handler;

	public ModRdnHandler(Handler handler) {
        this.handler = handler;
	}

	public int modrdn(PenroseSession session, String dn, String newRdn)
			throws Exception {

        int rc;
        try {

            log.warn("ModRDN \""+dn+"\" to \""+newRdn+"\".");

            log.debug("-------------------------------------------------------------------------------");
            log.debug("MODRDN:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - DN: " + dn);
            log.debug(" - New RDN: " + newRdn);

            if (session != null && session.getBindDn() == null) {
                PenroseConfig penroseConfig = handler.getPenroseConfig();
                ServiceConfig serviceConfig = penroseConfig.getServiceConfig("LDAP");
                String s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
                boolean allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();
                if (!allowAnonymousAccess) {
                    return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
                }
            }

            String ndn = LDAPDN.normalize(dn);

            Entry entry = handler.getFindHandler().find(session, ndn);
            if (entry == null) return LDAPException.NO_SUCH_OBJECT;

            rc = performModRdn(session, entry, newRdn);
            if (rc != LDAPException.SUCCESS) return rc;

            String parentDn = EntryUtil.getParentDn(dn);
            String newDn = newRdn+","+parentDn;

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_SUB);

            handler.getSearchHandler().search(
                    null,
                    newDn,
                    "(objectClass=*)",
                    sc,
                    results
            );

            EntryCache entryCache = handler.getEngine().getEntryCache();
            for (Iterator i=results.iterator(); i.hasNext(); ) {
                Entry e = (Entry)i.next();
                entryCache.put(e);
            }

            handler.getEngine().getEntryCache().remove(entry);

        } catch (LDAPException e) {
            rc = e.getResultCode();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;
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
            Entry entry,
            String newRdn)
			throws Exception {

        int rc = handler.getACLEngine().checkModify(session, entry.getDn(), entry.getEntryMapping());
        if (rc != LDAPException.SUCCESS) return rc;

        EntryMapping entryMapping = entry.getEntryMapping();
        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(entryMapping);

        if (partition.isProxy(entryMapping)) {
            log.debug("Renaming "+entry.getDn()+" via proxy");
            handler.getEngine().modrdnProxy(partition, entryMapping, entry, newRdn);
            return LDAPException.SUCCESS;
        }

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
