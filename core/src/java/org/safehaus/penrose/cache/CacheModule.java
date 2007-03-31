package org.safehaus.penrose.cache;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.Modification;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.changelog.ChangeLogUtil;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class CacheModule extends Module {

    public final static String SOURCE              = "source";
    public final static String CACHE               = "cache";
    public final static String CHANGELOG           = "changelog";
    public final static String TRACKER             = "tracker";

    public final static String INTERVAL            = "interval";
    public final static int DEFAULT_INTERVAL       = 30; // seconds

    public final static String TIMESTAMP           = "timestamp";

    public final static String INITIALIZE          = "initialize";
    public final static boolean DEFAULT_INITIALIZE = false;

    public final static String USER                = "user";

    public final static String SORTED              = "sorted";
    public final static boolean DEFAULT_SORTED     = false;

    protected String sourceName;
    protected String cacheName;
    protected String changeLogName;
    protected String trackerName;

    protected int interval; // second
    protected String timestamp;
    protected boolean initialize;
    protected boolean sorted;

    protected SourceManager sourceManager;
    protected Source source;
    protected Source cache;
    protected Source changeLog;
    protected Source tracker;
    protected String user;

    protected ChangeLogUtil changeLogUtil;
    protected CacheRunnable runnable;

    public void init() throws Exception {

        sourceName = getParameter(SOURCE);
        log.debug("Source: "+ sourceName);

        cacheName = getParameter(CACHE);
        log.debug("Cache: "+ cacheName);

        changeLogName = getParameter(CHANGELOG);
        log.debug("Change Log: "+changeLogName);

        trackerName = getParameter(TRACKER);
        log.debug("Tracker: "+trackerName);

        String s = getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+ getInterval());

        timestamp = getParameter(TIMESTAMP);
        log.debug("Timestamp: "+ timestamp);

        s = getParameter(INITIALIZE);
        initialize = s == null ? DEFAULT_INITIALIZE : Boolean.parseBoolean(s);
        log.debug("Initialize: "+ initialize);

        s = getParameter(SORTED);
        sorted = s == null ? DEFAULT_SORTED : Boolean.parseBoolean(s);
        log.debug("Sorted: "+ sorted);

        user = getParameter(USER);
        log.debug("User: "+user);

        sourceManager = penroseContext.getSourceManager();

        source = sourceManager.getSource(partition, sourceName);
        cache = sourceManager.getSource(partition, cacheName);

        if (changeLogName != null) {
            changeLog = sourceManager.getSource(partition, changeLogName);
            tracker = sourceManager.getSource(partition, trackerName);

            changeLogUtil = createChangeLogUtil();
            changeLogUtil.setSource(source);
            changeLogUtil.setCache(cache);
            changeLogUtil.setChangeLog(changeLog);
            changeLogUtil.setTracker(tracker);
            changeLogUtil.setUser(user);
        }
    }

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return null;
    }

    public void start() throws Exception {

        if (initialize) {
            try {
                cache.create();
            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }

            synchronize();
        }

        runnable = new CacheRunnable(this);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void stop() throws Exception {
        runnable.stop();
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public void process() throws Exception {
        if (changeLog == null) {
            synchronize();
        } else {
            changeLogUtil.synchronize();
        }
    }

    public void synchronize() throws Exception {
        if (sorted) {
            synchronizeSorted();
        } else {
            synchronizeUnsorted();
        }
    }

    public void synchronizeSorted() throws Exception {

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
                add(sourceEntry);
                sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;

            } else if (c > 0) {
                delete(cacheEntry);
                cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;

            } else {
                update(sourceEntry, cacheEntry);
                sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
                cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;
            }
        }

        while (sourceEntry != null) {
            add(sourceEntry);
            sourceEntry = sourceResponse.hasNext() ? (Entry)sourceResponse.next() : null;
        }

        while (cacheEntry != null) {
            delete(cacheEntry);
            cacheEntry = cacheResponse.hasNext() ? (Entry)cacheResponse.next() : null;
        }

        log.debug("Cache synchronization completed.");
    }

    public void synchronizeUnsorted() throws Exception {

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse sourceResponse = new SearchResponse();

        source.search(sourceRequest, sourceResponse);

        Map sourceEntries = new LinkedHashMap();
        while (sourceResponse.hasNext()) {
            Entry sourceEntry = (Entry)sourceResponse.next();
            sourceEntries.put(sourceEntry.getDn(), sourceEntry);
        }

        SearchRequest cacheRequest = new SearchRequest();
        SearchResponse cacheResponse = new SearchResponse();

        cache.search(cacheRequest, cacheResponse);

        Map cacheEntries = new LinkedHashMap();
        while (cacheResponse.hasNext()) {
            Entry cacheEntry = (Entry)cacheResponse.next();
            cacheEntries.put(cacheEntry.getDn(), cacheEntry);
        }

        for (Iterator i=sourceEntries.values().iterator(); i.hasNext(); ) {
            Entry sourceEntry = (Entry)i.next();
            DN dn = sourceEntry.getDn();

            Entry cacheEntry = (Entry)cacheEntries.get(dn);

            if (cacheEntry == null) {
                add(sourceEntry);
            } else {
                update(sourceEntry, cacheEntry);
            }
        }

        for (Iterator i=cacheEntries.values().iterator(); i.hasNext(); ) {
            Entry cacheEntry = (Entry)i.next();
            DN dn = cacheEntry.getDn();

            if (!sourceEntries.containsKey(dn)) {
                delete(cacheEntry);
            }
        }

        log.debug("Cache synchronization completed.");
    }

    public void add(Entry entry) throws Exception {
        if (log.isDebugEnabled()) log.debug("Adding "+entry.getDn()+" to cache.");
        cache.add(entry.getDn(), entry.getAttributes());
    }

    public void delete(Entry entry) throws Exception {
        if (log.isDebugEnabled()) log.debug("Deleting "+entry.getDn()+" from cache.");
        cache.delete(entry.getDn());
    }

    public void update(Entry sourceEntry, Entry cacheEntry) throws Exception {

        if (log.isDebugEnabled()) log.debug("Updating "+sourceEntry.getDn()+" on cache.");

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
    }

    public boolean isSorted() {
        return sorted;
    }

    public void setSorted(boolean sorted) {
        this.sorted = sorted;
    }
}
