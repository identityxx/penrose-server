package org.safehaus.penrose.cache;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.*;

import java.util.Iterator;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SnapshotCacheModule extends CacheModule {

    public final static String TIMESTAMP = "timestamp";

    public String timestamp;

    public void init() throws Exception {

        super.init();

        timestamp = getParameter(TIMESTAMP);
        log.debug("Timestamp: "+ timestamp);
    }

    public void process() throws Exception {

        boolean debug = log.isDebugEnabled();

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse sourceResponse = new SearchResponse();

        source.search(sourceRequest, sourceResponse);

        SearchRequest cacheRequest = new SearchRequest();
        SearchResponse cacheResponse = new SearchResponse();

        cache.search(cacheRequest, cacheResponse);

        Entry sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
        Entry cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;

        while (sourceEntry != null && cacheEntry != null) {
            int c = sourceEntry.getDn().compareTo(cacheEntry.getDn());

            if (c  < 0) {
                if (debug) log.debug("Adding "+sourceEntry.getDn()+" to cache.");
                cache.add(sourceEntry.getDn(), sourceEntry.getAttributes());

                sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;

            } else if (c > 0) {
                if (debug) log.debug("Deleting "+cacheEntry.getDn()+" from cache.");
                cache.delete(cacheEntry.getDn());

                cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;

            } else {
                if (debug) log.debug("Updating "+cacheEntry.getDn()+" on cache.");

                Collection<Modification> modifications = new ArrayList<Modification>();

                Attributes sourceAttributes = sourceEntry.getAttributes();
                for (Iterator i=sourceAttributes.getAll().iterator(); i.hasNext(); ) {
                    Attribute attribute = (Attribute)i.next();
                    modifications.add(new Modification(Modification.REPLACE, attribute));
                }

                Attributes cacheAttributes = cacheEntry.getAttributes();
                for (Iterator i=cacheAttributes.getAll().iterator(); i.hasNext(); ) {
                    Attribute attribute = (Attribute)i.next();
                    if (sourceAttributes.get(attribute.getName()) != null) continue;
                    modifications.add(new Modification(Modification.DELETE, attribute));
                }

                cache.modify(cacheEntry.getDn(), modifications);

                sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
                cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;
            }
        }

        while (sourceEntry != null) {
            if (debug) log.debug("Adding "+sourceEntry.getDn()+" to cache.");
            cache.add(sourceEntry.getDn(), sourceEntry.getAttributes());

            sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
        }

        while (cacheEntry != null) {
            if (debug) log.debug("Deleting "+cacheEntry.getDn()+" from cache.");
            cache.delete(cacheEntry.getDn());

            cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;
        }

        log.debug("Cache synchronization completed.");
    }
}
