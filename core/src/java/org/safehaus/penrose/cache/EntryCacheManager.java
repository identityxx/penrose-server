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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCacheManager {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String DEFAULT_CACHE_NAME  = "Entry Cache";
    public final static String DEFAULT_CACHE_CLASS = DefaultEntryCache.class.getName();

    CacheConfig cacheConfig;

    private ConnectionManager connectionManager;

    public Map caches = new TreeMap();
    public Collection listeners = new ArrayList();

    public EntryCache create(Partition partition, EntryMapping entryMapping) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        CacheConfig cacheConfig = partitionConfig.getEntryCacheConfig();

        String cacheClass = cacheConfig.getCacheClass();
        log.debug("Creating entry cache for "+entryMapping.getDn());

        Class clazz = Class.forName(cacheClass);
        EntryCache entryCache = (EntryCache)clazz.newInstance();

        entryCache.setCacheConfig(cacheConfig);
        entryCache.setConnectionManager(connectionManager);
        entryCache.setPartition(partition);
        entryCache.setEntryMapping(entryMapping);

        entryCache.init();

        Map map = (Map)caches.get(partition.getName());
        if (map == null) {
            map = new TreeMap();
            caches.put(partition.getName(), map);
        }
        map.put(entryMapping.getDn().toLowerCase(), entryCache);

        return entryCache;
    }

    public void addListener(EntryCacheListener listener) {
        listeners.add(listener);
    }

    public void removeListener(EntryCacheListener listener) {
        listeners.remove(listener);
    }

    public Collection getParameterNames() {
        return cacheConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return cacheConfig.getParameter(name);
    }

    public void init() throws Exception {
    }

    public EntryCache get(Partition partition, EntryMapping entryMapping) throws Exception {
        Map map = (Map)caches.get(partition.getName());
        if (map == null) return null;
        return (EntryCache)map.get(entryMapping.getDn().toLowerCase());
    }

    public void remove(Partition partition, EntryMapping entryMapping, String dn) throws Exception {
        log.info("["+entryMapping.getDn()+"] remove("+dn+")");

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();
            EntryCache cache = get(partition, childMapping);

            PenroseSearchResults childDns = new PenroseSearchResults();
            cache.search(dn, null, childDns);
            childDns.close();

            for (Iterator j=childDns.iterator(); j.hasNext(); ) {
                String childDn = (String)j.next();

                remove(partition, childMapping, childDn);
            }
        }

        log.debug("Remove cache "+dn);

        get(partition, entryMapping).remove(dn);
    }

    public void create(Partition partition) throws Exception {
        Collection entryMappings = partition.getRootEntryMappings();

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        EntryCache cache = get(partition, entryMapping);

        cache.globalCreate();

        create(partition, entryMappings);
    }

    public  void create(Partition partition, Collection entryDefinitions) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryCache cache = get(partition, entryMapping);

            log.debug("Creating tables for "+entryMapping.getDn());
            cache.create();

            Collection children = partition.getChildren(entryMapping);
            create(partition, children);
        }
    }

    public void load(Penrose penrose, Partition partition) throws Exception {

        Collection entryMappings = partition.getRootEntryMappings();

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Loading entries under "+entryMapping.getDn());

            PenroseSession adminSession = penrose.newSession();
            adminSession.setBindDn(penrose.getPenroseConfig().getRootDn());

            PenroseSearchResults sr = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_SUB);

            adminSession.search(
                    entryMapping.getDn(),
                    "(objectClass=*)",
                    sc,
                    sr
            );

            while (sr.hasNext()) sr.next();

            adminSession.close();
        }
    }

    public void clean(Partition partition) throws Exception {
        Collection entryMappings = partition.getRootEntryMappings();
        clean(partition, entryMappings);
    }

    public void clean(
            final Partition partition,
            final Collection entryDefinitions
    ) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            final EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            clean(partition, children);

            EntryCache entryCache = get(partition, entryMapping);
            entryCache.clean();
        }
    }

    public void invalidate(Partition partition) throws Exception {
        Collection entryMappings = partition.getRootEntryMappings();
        invalidate(partition, entryMappings);
    }

    public void invalidate(
            final Partition partition,
            final Collection entryDefinitions
    ) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            final EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            invalidate(partition, children);

            EntryCache entryCache = get(partition, entryMapping);
            entryCache.invalidate();
        }
    }

    public void drop(Partition partition) throws Exception {
        Collection entryMappings = partition.getRootEntryMappings();
        drop(partition, entryMappings);

        EntryMapping entryMapping = (EntryMapping)entryMappings.iterator().next();
        EntryCache cache = get(partition, entryMapping);

        cache.globalDrop();
    }

    public EntryCache drop(Partition partition, Collection entryDefinitions) throws Exception {

        EntryCache cache = null;

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            drop(partition, children);

            cache = get(partition, entryMapping);
            log.debug("Dropping tables for "+entryMapping.getDn());
            cache.drop();
        }

        return cache;
    }

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public void postEvent(EntryCacheEvent event) throws Exception {

        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            EntryCacheListener listener = (EntryCacheListener)i.next();

            try {
                switch (event.getType()) {
                    case EntryCacheEvent.CACHE_ADDED:
                        listener.cacheAdded(event);
                        break;
                    case EntryCacheEvent.CACHE_REMOVED:
                        listener.cacheRemoved(event);
                        break;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }
}
