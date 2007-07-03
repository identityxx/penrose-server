/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.EntryCacheListener;
import org.safehaus.penrose.cache.EntryCacheEvent;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.entry.*;

import javax.naming.directory.*;
import javax.naming.directory.Attributes;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class LDAPSyncModule extends Module implements EntryCacheListener {

    public final static String CONNECTION = "connection";

    public String connectionName;

    public void init() throws Exception {

        connectionName = getParameter(CONNECTION);

        HandlerManager handlerManager = sessionContext.getHandlerManager();
        Handler handler = handlerManager.getHandler(partition);
        EntryCache entryCache = handler.getEntryCache();

        entryCache.addListener(this);
    }

    public DirContext getConnection() throws Exception {

        ConnectionManager connectionManager = penroseContext.getConnectionManager();

        LDAPClient client = (LDAPClient)connectionManager.openConnection(partition, connectionName);
        return client.open();
    }

    public void cacheAdded(EntryCacheEvent event) throws Exception {

        Entry entry = (Entry)event.getSource();

        if (!partition.contains(entry.getDn())) return;

        DirContext ctx = null;

        try {
            ctx = getConnection();

            DN baseDn = entry.getDn();
            log.debug("Adding "+baseDn);

            javax.naming.directory.SearchResult searchResult = createSearchResult(entry);
            Attributes attributes = searchResult.getAttributes();
            ctx.createSubcontext(searchResult.getName(), attributes);

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public javax.naming.directory.SearchResult createSearchResult(Entry entry) {

        //log.debug("Converting "+entry.getDn());

        org.safehaus.penrose.ldap.Attributes attributes = entry.getAttributes();
        javax.naming.directory.Attributes attrs = new javax.naming.directory.BasicAttributes();

        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String name = attribute.getName();
            Collection values = attribute.getValues();

            javax.naming.directory.Attribute attr = new javax.naming.directory.BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();

                //String className = value.getClass().getName();
                //className = className.substring(className.lastIndexOf(".")+1);
                //log.debug(" - "+name+": "+value+" ("+className+")");

                if (value instanceof byte[]) {
                    attr.add(value);

                } else {
                    attr.add(value.toString());
                }
            }

            attrs.put(attr);
        }

        return new javax.naming.directory.SearchResult(entry.getDn().toString(), entry, attrs);
    }

    public void cacheRemoved(EntryCacheEvent event) throws Exception {

        DN baseDn = (DN)event.getSource();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        Partition partition = partitionManager.getPartition(baseDn);
        Collection entryMappings = partition.findEntryMappings(baseDn);

        if (entryMappings == null || entryMappings.isEmpty()) return;

        DirContext ctx = null;

        try {
            ctx = getConnection();

            log.debug("Removing "+baseDn);
            ctx.destroySubcontext(baseDn.toString());

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }
}
