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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.handler.SessionHandler;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPSearchConstraints;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PersistentEntryCache extends EntryCache {

    public EntryCacheStorage createCacheStorage(String parentDn, EntryMapping entryMapping) throws Exception {

        Partition partition = partitionManager.getPartition(entryMapping);

        EntryCacheStorage cacheStorage = new PersistentEntryCacheStorage();
        cacheStorage.setCacheConfig(getCacheConfig());
        cacheStorage.setConnectionManager(connectionManager);
        cacheStorage.setPartition(partition);
        cacheStorage.setEntryMapping(entryMapping);
        cacheStorage.setParentDn(parentDn);

        cacheStorage.init();

        return cacheStorage;
    }

    public void load(Penrose penrose, Partition partition) throws Exception {

        SessionHandler sessionHandler = penrose.getSessionHandler();
        Collection entryMappings = partition.getRootEntryMappings();

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Loading entries under "+entryMapping.getDn());

            PenroseSearchResults sr = sessionHandler.search(
                    null,
                    entryMapping.getDn(),
                    LDAPConnection.SCOPE_SUB,
                    LDAPSearchConstraints.DEREF_NEVER,
                    "(objectClass=*)",
                    new ArrayList()
            );

            while (sr.hasNext()) sr.next();
        }
    }
}
