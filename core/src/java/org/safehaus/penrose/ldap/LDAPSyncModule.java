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
package org.safehaus.penrose.ldap;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.EntryCacheListener;
import org.safehaus.penrose.cache.EntryCacheEvent;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.handler.Handler;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class LDAPSyncModule extends Module implements EntryCacheListener {

    public final static String CONNECTION = "connection";

    public PartitionManager partitionManager;
    public ConnectionManager connectionManager;

    public String connectionName;

    public void init() throws Exception {

        connectionName = getParameter(CONNECTION);

        partitionManager = penrose.getPartitionManager();
        connectionManager = penrose.getConnectionManager();

        Handler handler = penrose.getHandler();
        EntryCache entryCache = handler.getEntryCache();

        entryCache.addListener(this);
    }

    public DirContext getConnection() throws Exception {
        LDAPClient client = (LDAPClient)connectionManager.openConnection(connectionName);
        return client.getContext();
    }

    public void cacheAdded(EntryCacheEvent event) throws Exception {

        Entry entry = (Entry)event.getSource();
        EntryMapping entryMapping = entry.getEntryMapping();

        if (!partition.containsEntryMapping(entryMapping)) return;

        DirContext ctx = null;

        try {
            ctx = getConnection();

            String baseDn = entry.getDn();
            log.debug("Adding "+baseDn);

            PenroseSession adminSession = penrose.newSession();
            adminSession.setBindDn(penrose.getPenroseConfig().getRootDn());

            PenroseSearchResults sr = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_SUB);

            adminSession.search(
                    baseDn,
                    "(objectClass=*)",
                    sc,
                    sr
            );

            while (sr.hasNext()) {
                SearchResult ldapEntry = (SearchResult)sr.next();

                String dn = ldapEntry.getName();
                log.debug(" - "+dn);

                Attributes attributes = ldapEntry.getAttributes();

                try {
                    ctx.createSubcontext(dn, attributes);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }

            adminSession.close();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void cacheRemoved(EntryCacheEvent event) throws Exception {

        String baseDn = (String)event.getSource();
        Partition partition = partitionManager.findPartition(baseDn);
        EntryMapping entryMapping = partition.findEntryMapping(baseDn);
        
        if (entryMapping == null) return;

        DirContext ctx = null;

        try {
            ctx = getConnection();

            log.debug("Removing "+baseDn);

            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sc.setReturningAttributes(new String[] { "dn" });

            NamingEnumeration ne = ctx.search(baseDn, "(objectClass=*)", sc);

            ArrayList dns = new ArrayList();
            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                String name = sr.getName();

                String dn = "".equals(name) ? baseDn : name+","+baseDn;
                dns.add(0, dn);
            }

            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                String dn = (String)i.next();
                log.debug(" - "+dn);

                try {
                    ctx.destroySubcontext(dn);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }
}
