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
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.handler.Handler;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPSearchConstraints;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCacheManager {

    Logger log = Logger.getLogger(getClass());

    public final static String DEFAULT_CACHE_NAME  = "Entry Cache";
    public final static String DEFAULT_CACHE_CLASS = DefaultEntryCache.class.getName();

    private ConnectionManager connectionManager;
    private PenroseConfig penroseConfig;
    private PartitionManager partitionManager;

    private CacheConfig cacheConfig;

    private String cacheClass;
    private Map caches = new TreeMap();

    boolean first = true;

    public void init() throws Exception {
        cacheConfig = penroseConfig.getEntryCacheConfig();
        cacheClass = cacheConfig.getCacheClass() == null ? DEFAULT_CACHE_CLASS : cacheConfig.getCacheClass();
    }

    public EntryCache getCache(String parentDn, EntryMapping entryMapping) throws Exception {

        String key = entryMapping.getRdn()+","+parentDn;
        EntryCache cache = (EntryCache)caches.get(key);

        if (cache == null) {

            Class clazz = Class.forName(cacheClass);
            cache = (EntryCache)clazz.newInstance();

            cache.setCacheConfig(cacheConfig);

            cache.setConnectionManager(connectionManager);

            Partition partition = partitionManager.getPartition(entryMapping);
            cache.setPartition(partition);
            cache.setEntryMapping(entryMapping);
            cache.setParentDn(parentDn);

            cache.init();

            caches.put(key, cache);
        }

        return cache;
    }

    public void put(EntryMapping entryMapping, String parentDn, Filter filter, Collection dns) throws Exception {
        getCache(parentDn, entryMapping).put(filter, dns);
    }

    public Collection search(EntryMapping entryMapping, String parentDn, Filter filter) throws Exception {
        return getCache(parentDn, entryMapping).search(filter);
    }

    public void put(Entry entry) throws Exception {
        getCache(entry.getParentDn(), entry.getEntryMapping()).put(entry.getRdn(), entry);
    }

    public Entry get(String dn) throws Exception {
        EntryMapping entryMapping = partitionManager.findEntryMapping(dn);
        String parentDn = Entry.getParentDn(dn);
        Row rdn = Entry.getRdn(dn);
        return getCache(parentDn, entryMapping).get(rdn);
    }

    public Entry get(EntryMapping entryMapping, String parentDn, Row rdn) throws Exception {
        return getCache(parentDn, entryMapping).get(rdn);
    }

    public void remove(Entry entry) throws Exception {
        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = partitionManager.getPartition(entryMapping);
        remove(partition, entryMapping, entry.getParentDn(), entry.getRdn());
    }

    public void remove(Partition partition, EntryMapping entryMapping, String parentDn, Row rdn) throws Exception {
        String dn = rdn+","+parentDn;

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();

            Collection childDns = search(childMapping, dn, null);
            for (Iterator j=childDns.iterator(); j.hasNext(); ) {
                String childDn = (String)j.next();

                Row childRdn = Entry.getRdn(childDn);
                remove(partition, childMapping, dn, childRdn);
            }
        }

        log.debug("Remove cache "+dn);
        getCache(parentDn, entryMapping).remove(rdn);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public void create() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            create(partition, null, entryDefinitions);
        }
    }

    public  void create(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryCache cache = getCache(parentDn, entryMapping);

            if (first) {
                cache.globalCreate();
                first = false;
            }

            log.debug("Creating tables for "+entryMapping.getDn());
            cache.create();

            Collection children = partition.getChildren(entryMapping);
            create(partition, entryMapping.getDn(), children);
        }
    }

    public void load(Penrose penrose) throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            load(penrose, partition);
        }
    }

    public void load(Penrose penrose, Partition partition) throws Exception {

        Handler handler = penrose.getHandler();
        Collection entryDefinitions = partition.getRootEntryMappings();

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Loading entries under "+entryMapping.getDn());

            PenroseSearchResults sr = handler.search(
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

    public void clean() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            clean(partition, null, entryDefinitions);
        }
    }

    public void clean(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryCache entryCache = getCache(parentDn, entryMapping);
            Collection dns = entryCache.search(null);

            Collection children = partition.getChildren(entryMapping);
            for (Iterator j=dns.iterator(); j.hasNext(); ) {
                String dn = (String)j.next();
                clean(partition, dn, children);

                Row rdn = Entry.getRdn(dn);
                entryCache.remove(rdn);
            }
        }
    }

    public void drop() throws Exception {

        EntryCache cache = null;

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            cache = drop(partition, null, entryDefinitions);
        }

        cache.globalDrop();
    }

    public EntryCache drop(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {

        EntryCache cache = null;

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            drop(partition, entryMapping.getDn(), children);

            cache = getCache(parentDn, entryMapping);
            log.debug("Dropping tables for "+entryMapping.getDn());
            cache.drop();
        }

        return cache;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }
}
