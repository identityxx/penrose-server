/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.event.CacheEvent;
import org.safehaus.penrose.event.CacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceCache {

    Logger log = LoggerFactory.getLogger(getClass());

    private Cache cache;
    private CacheContext cacheContext;

    private int size;

    private Map records = new LinkedHashMap();

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();

        String s = cache.getParameter("size");
        size = s == null ? 50 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public void refresh() throws Exception {
    }

    public Date getExpiration(Source source) throws Exception {
        return null;
    }

    public void setExpiration(Source source, Date date) throws Exception {
    }

    public Map getMap(Source source) {
        Map map = (Map)records.get(source.getSourceName());
        if (map == null) {
            map = new TreeMap();
            records.put(source.getSourceName(), map);
        }
        return map;
    }

    public AttributeValues get(Source source, Row pk) throws Exception {
        Map map = getMap(source);
        Row npk = cacheContext.getSchema().normalize(pk);

        log.debug("Getting source cache ("+map.size()+"): "+npk);

        AttributeValues values = (AttributeValues)map.get(npk);
        if (values != null) return values;

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();

            if (cacheContext.getSchema().match(key, npk)) {
                values = (AttributeValues)map.remove(key);
                map.put(key, values);
                return values;
            }
        }

        return null;
    }

    public Map getByPks(Source source, Collection pks) throws Exception {

        Map results = new HashMap();

        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            AttributeValues values = get(source, pk);
            if (values == null) continue;

            results.put(pk, values);
        }

        return results;
    }

    public Map get(
            Source source,
            Collection filters)
            throws Exception {

        Map results = new TreeMap();
        Map map = getMap(source);
        //log.debug("PKs in cache: "+map.keySet());

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            AttributeValues attributeValues = (AttributeValues)map.get(pk);

            for (Iterator j=filters.iterator(); j.hasNext(); ) {
                Row filter = (Row)j.next();

                boolean found = cacheContext.getSchema().partialMatch(attributeValues, filter);

                if (found) results.put(pk, attributeValues);
            }

        }

        return results;
    }

    public void put(Source source, Row pk, AttributeValues values) throws Exception {

        Map map = getMap(source);
        Row npk = cacheContext.getSchema().normalize(pk);

        while (map.size() >= size) {
            log.debug("Trimming source cache ("+map.size()+").");
            Object key = map.keySet().iterator().next();
            map.remove(key);
        }

        log.debug("Storing source cache ("+map.size()+"): "+npk);
        map.put(npk, values);
    }

    public void remove(Source source, Row pk) throws Exception {
        Map map = getMap(source);
        Row npk = cacheContext.getSchema().normalize(pk);

        log.debug("Removing source cache ("+map.size()+"): "+npk);
        
        AttributeValues values = (AttributeValues)map.get(npk);
        if (values != null) {
            map.remove(npk);
            return;
        }

        Collection keys = new ArrayList();
        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();
            if (cacheContext.getSchema().match(key, npk)) {
                keys.add(key);
            }
        }

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Row key = (Row)i.next();
            map.remove(key);
        }
    }

    public void postCacheEvent(SourceDefinition sourceConfig, CacheEvent event)
            throws Exception {
        List listeners = sourceConfig.getListeners();

        for (Iterator i = listeners.iterator(); i.hasNext();) {
            Object listener = i.next();
            if (!(listener instanceof CacheListener))
                continue;

            CacheListener cacheListener = (CacheListener) listener;
            switch (event.getType()) {
            case CacheEvent.BEFORE_LOAD_ENTRIES:
                cacheListener.beforeLoadEntries(event);
                break;

            case CacheEvent.AFTER_LOAD_ENTRIES:
                cacheListener.afterLoadEntries(event);
                break;
            }
        }
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
