/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPDN;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCache {

    Logger log = LoggerFactory.getLogger(getClass());

    private Cache cache;
    private CacheContext cacheContext;

    private int size;

    private Map entries = new LinkedHashMap();

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();

        String s = cache.getParameter("size");
        size = s == null ? 50 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Entry get(String dn) throws Exception {

        String ndn = cacheContext.getSchema().normalize(dn);

        log.debug("Getting entry cache ("+entries.size()+"): "+ndn);

        Entry entry = (Entry)entries.remove(ndn);
        entries.put(ndn, entry);

        return entry;
    }

    public void put(Entry entry) throws Exception {

        String dn = entry.getDn();
        String ndn = cacheContext.getSchema().normalize(dn);

        while (entries.size() >= size) {
            log.debug("Trimming entry cache ("+entries.size()+").");
            String key = (String)entries.keySet().iterator().next();
            entries.remove(key);
        }

        log.debug("Storing entry cache ("+entries.size()+"): "+ndn);
        entries.put(ndn, entry);
    }

    public void remove(Entry entry) throws Exception {

        String dn = entry.getDn();
        String ndn = cacheContext.getSchema().normalize(dn);

        log.debug("Removing entry cache ("+entries.size()+"): "+ndn);
        entries.remove(ndn);
    }

    public void invalidate(String dn) throws Exception {
        String ndn = cacheContext.getSchema().normalize(dn);
    }
    
    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public Map getEntries() {
        return entries;
    }

    public void setEntries(Map entries) {
        this.entries = entries;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
