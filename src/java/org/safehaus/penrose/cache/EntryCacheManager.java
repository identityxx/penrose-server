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
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.Penrose;
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

    private PenroseConfig penroseConfig;
    private PartitionManager partitionManager;
    private Engine engine;

    private Map caches = new TreeMap();

    public EntryCache getCache(String parentDn, EntryMapping entryMapping) throws Exception {
        String cacheName = entryMapping.getParameter(EntryMapping.CACHE);
        cacheName = cacheName == null ? EntryMapping.DEFAULT_CACHE : cacheName;
        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();

        String key = entryMapping.getRdn()+","+parentDn;

        EntryCache cache = (EntryCache)caches.get(key);

        if (cache == null) {

            String cacheClass = cacheConfig.getCacheClass();
            cacheClass = cacheClass == null ? EngineConfig.DEFAULT_CACHE_CLASS : cacheClass;

            Class clazz = Class.forName(cacheClass);
            cache = (EntryCache)clazz.newInstance();

            cache.setParentDn(parentDn);
            cache.setEntryMapping(entryMapping);
            cache.setEngine(engine);
            cache.init(cacheConfig);

            caches.put(key, cache);
        }

        return cache;
    }

    public void remove(Entry entry) throws Exception {
        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = partitionManager.getPartition(entryMapping);
        remove(partition, entry.getParentDn(), entryMapping, entry.getRdn());
    }

    public void remove(Partition partition, String parentDn, EntryMapping entryMapping, Row rdn) throws Exception {
        String dn = rdn+","+parentDn;

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();

            Collection childDns = getCache(dn, childMapping).search(null);
            for (Iterator j=childDns.iterator(); j.hasNext(); ) {
                String childDn = (String)j.next();

                Row childRdn = Entry.getRdn(childDn);
                remove(partition, dn, childMapping, childRdn);
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

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
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

            log.debug("Creating tables for "+entryMapping.getDn());
            EntryCache cache = getCache(parentDn, entryMapping);
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

            Collection children = partition.getChildren(entryMapping);
            clean(partition, entryMapping.getDn(), children);

            log.debug("Cleaning tables for "+entryMapping.getDn());
            EntryCache cache = getCache(parentDn, entryMapping);
            cache.clean();
        }
    }

    public void drop() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            drop(partition, null, entryDefinitions);
        }
    }

    public void drop(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            drop(partition, entryMapping.getDn(), children);

            log.debug("Deleting entries under "+entryMapping.getDn());
            EntryCache cache = getCache(parentDn, entryMapping);
            cache.drop();
        }
    }
}
