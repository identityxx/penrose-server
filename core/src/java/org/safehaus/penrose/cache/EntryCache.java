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
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.filter.Filter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCache {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String DEFAULT_CACHE_NAME  = "Entry Cache";
    public final static String DEFAULT_CACHE_CLASS = DefaultEntryCache.class.getName();

    CacheConfig cacheConfig;

    Penrose penrose;

    public Map caches = new TreeMap();
    public Collection listeners = new ArrayList();

    public EntryCacheStorage createCacheStorage(Partition partition, EntryMapping entryMapping) throws Exception {

        EntryCacheStorage cacheStorage = new EntryCacheStorage(penrose);
        cacheStorage.setCacheConfig(cacheConfig);
        cacheStorage.setPartition(partition);
        cacheStorage.setEntryMapping(entryMapping);

        cacheStorage.init();

        return cacheStorage;
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

    public EntryCacheStorage getCacheStorage(Partition partition, EntryMapping entryMapping) throws Exception {

        String key = entryMapping.getDn();
        //log.debug("Getting cache storage for "+key);
        //log.debug("entry cache: "+caches.keySet());

        EntryCacheStorage cacheStorage = (EntryCacheStorage)caches.get(key);

        if (cacheStorage == null) {
            //log.debug("Creating new cache storage.");
            cacheStorage = createCacheStorage(partition, entryMapping);
            caches.put(key, cacheStorage);
        }

        return cacheStorage;
    }

    public void add(Partition partition, EntryMapping entryMapping, Filter filter, String dn) throws Exception {
        log.info("["+entryMapping.getDn()+"] add("+filter+", "+dn+")");

        getCacheStorage(partition, entryMapping).add(filter, dn);
    }
    
    public void search(Partition partition, EntryMapping entryMapping, SourceConfig sourceConfig, Row filter, PenroseSearchResults results) throws Exception {
        log.info("["+entryMapping.getDn()+"] search("+sourceConfig.getName()+", "+filter+")");

        getCacheStorage(partition, entryMapping).search(sourceConfig, filter, results);
    }

    public boolean contains(Partition partition, EntryMapping entryMapping, String parentDn, Filter filter) throws Exception {
        log.info("["+entryMapping.getDn()+"] contains("+parentDn+", "+filter+")");

        return getCacheStorage(partition, entryMapping).contains(parentDn, filter);
    }

    public void update(Partition partition, EntryMapping entryMapping, PenroseSearchResults results) throws Exception {
        log.info("["+entryMapping.getDn()+"] update()");

        getCacheStorage(partition, entryMapping).search(null, (Filter)null, results);
    }

    public boolean search(
            Partition partition,
            EntryMapping entryMapping,
            String parentDn,
            Filter filter,
            PenroseSearchResults results)
            throws Exception {

        log.info("["+entryMapping.getDn()+"] search("+parentDn+", "+filter+")");

        return getCacheStorage(partition, entryMapping).search(parentDn, filter, results);
    }

    public void put(Partition partition, EntryMapping entryMapping, Entry entry) throws Exception {
        log.info("["+entryMapping.getDn()+"] put("+entry.getDn()+")");

        getCacheStorage(partition, entryMapping).put(entry.getDn(), entry);

        EntryCacheEvent event = new EntryCacheEvent(entry, EntryCacheEvent.CACHE_ADDED);
        postEvent(event);
    }

    public void put(Partition partition, EntryMapping entryMapping, Filter filter, Collection dns) throws Exception {
        log.info("["+entryMapping.getDn()+"] put("+filter+", "+dns+")");

        getCacheStorage(partition, entryMapping).put(filter, dns);
    }

    public Entry get(Partition partition, EntryMapping entryMapping, String dn) throws Exception {
        log.info("["+entryMapping.getDn()+"]: get("+dn+")");

        return getCacheStorage(partition, entryMapping).get(dn);
    }

    public void remove(Partition partition, EntryMapping entryMapping, String dn) throws Exception {
        log.info("["+entryMapping.getDn()+"] remove("+dn+")");

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();

            PenroseSearchResults childDns = new PenroseSearchResults();
            search(partition, childMapping, dn, null, childDns);

            for (Iterator j=childDns.iterator(); j.hasNext(); ) {
                String childDn = (String)j.next();

                remove(partition, childMapping, childDn);
            }
        }

        log.debug("Remove cache "+dn);

        getCacheStorage(partition, entryMapping).remove(dn);

        EntryCacheEvent event = new EntryCacheEvent(dn, EntryCacheEvent.CACHE_REMOVED);
        postEvent(event);
    }

    public void create() throws Exception {
    }

    public  void create(Partition partition, Collection entryDefinitions) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryCacheStorage cacheStorage = getCacheStorage(partition, entryMapping);

            log.debug("Creating tables for "+entryMapping.getDn());
            cacheStorage.create();

            Collection children = partition.getChildren(entryMapping);
            create(partition, children);
        }
    }

    public void load(Penrose penrose, Partition partition) throws Exception {
    }

    public void clean(
            final Partition partition,
            final Collection entryDefinitions
    ) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            final EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            clean(partition, children);

            EntryCacheStorage entryCacheStorage = getCacheStorage(partition, entryMapping);
            if (!entryCacheStorage.contains(null, (Filter)null)) continue;

            PenroseSearchResults dns = new PenroseSearchResults();

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        String dn = (String)event.getObject();
                        remove(partition, entryMapping, dn);

                    } catch (Exception e) {
                        log.debug(e.getMessage(), e);
                    }
                }
            });

            entryCacheStorage.search(null, (Filter)null, dns);
        }
    }

    public void drop() throws Exception {
    }

    public EntryCacheStorage drop(Partition partition, Collection entryDefinitions) throws Exception {

        EntryCacheStorage cacheStorage = null;

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            drop(partition, children);

            cacheStorage = getCacheStorage(partition, entryMapping);
            log.debug("Dropping tables for "+entryMapping.getDn());
            cacheStorage.drop();
        }

        return cacheStorage;
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

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }
}
