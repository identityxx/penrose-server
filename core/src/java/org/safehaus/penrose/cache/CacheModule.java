package org.safehaus.penrose.cache;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.Modification;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.safehaus.penrose.engine.TransformEngine;

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
    //protected Source cache;
    protected Source changeLog;
    protected Source tracker;
    protected String user;

    protected Map<String,Source> caches = new LinkedHashMap<String,Source>();
    protected Map<String,Source> snapshots = new LinkedHashMap<String,Source>();

    protected ChangeLogUtil changeLogUtil;
    protected CacheRunnable runnable;

    public void init() throws Exception {

        boolean debug = log.isDebugEnabled();

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
        //cache = sourceManager.getSource(partition, cacheName);

        if (debug) log.debug("Caches for "+sourceName+":");

        Collection sources = sourceManager.getSources(partition);
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source src = (Source)i.next();

            Source cache = null;
            for (Iterator j=src.getFields().iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();

                String variable = field.getVariable();
                if (variable != null && variable.startsWith(sourceName+".")) {
                    log.debug(" - "+src.getName());
                    cache = src;
                    break;
                }

                Expression expression = field.getExpression();
                if (expression != null) {
                    String foreach = expression.getForeach();
                    if (foreach != null && foreach.startsWith(sourceName+".")) {
                        log.debug(" - "+src.getName());
                        cache = src;
                        break;
                    }
                }
            }

            if (cache == null) continue;

            caches.put(src.getName(), cache);
            Source snapshot = createSnapshotSource(cache);
            snapshots.put(src.getName(), snapshot);
        }

        if (changeLogName != null) {
            changeLog = sourceManager.getSource(partition, changeLogName);
            tracker = sourceManager.getSource(partition, trackerName);

            changeLogUtil = createChangeLogUtil();
            changeLogUtil.setSource(source);
            //changeLogUtil.setCache(cache);
            changeLogUtil.setChangeLog(changeLog);
            changeLogUtil.setTracker(tracker);
            changeLogUtil.setUser(user);
        }
    }

    public Source createSnapshotSource(Source cache) {
        String tableName = cache.getParameter(JDBCClient.TABLE);

        Source snapshot = (Source)cache.clone();
        snapshot.setName(cache.getName()+"_snapshot");
        snapshot.setParameter(JDBCClient.TABLE, tableName+"_snapshot");
        return snapshot;
    }

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return null;
    }

    public void start() throws Exception {

        if (initialize) {
            try {
                //cache.create();
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

        //cache.search(cacheRequest, cacheResponse);

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

        boolean debug = log.isDebugEnabled();

        SearchRequest sourceRequest = new SearchRequest();
        SearchResponse sourceResponse = new SearchResponse();

        source.search(sourceRequest, sourceResponse);

        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Source snapshot = (Source)snapshots.get(name);
            try {
                snapshot.create();
            } catch (Exception e) {
                snapshot.clean();
            }
        }

        Interpreter interpreter = penroseContext.getInterpreterManager().newInstance();
        RDNBuilder rb = new RDNBuilder();

        Map sourceEntries = new LinkedHashMap();
        while (sourceResponse.hasNext()) {
            Entry sourceEntry = (Entry)sourceResponse.next();

            DN dn = sourceEntry.getDn();
            Attributes attributes = sourceEntry.getAttributes();

            if (debug) {
                log.debug("Synchronizing "+dn);
            }

            sourceEntries.put(dn, sourceEntry);

            AttributeValues sourceValues = new AttributeValues();
            for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
                Attribute attribute = (Attribute)i.next();
                sourceValues.set(sourceName+"."+attribute.getName(), attribute.getValues());
            }

            interpreter.set(sourceValues);

            for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Source snapshot = (Source)snapshots.get(name);

                Attributes newAttributes = new Attributes();
                Attributes newRdns = new Attributes();

                for (Iterator j=snapshot.getFields().iterator(); j.hasNext(); ) {
                    Field field = (Field)j.next();
                    String fieldName = field.getName();

                    Object value = interpreter.eval(field);
                    if (value == null) continue;

                    if (value instanceof Collection) {
                        Collection list = (Collection)value;
                        newAttributes.addValues(fieldName, list);
                        if (field.isPrimaryKey()) newRdns.addValues(fieldName, list);
                    } else {
                        newAttributes.addValue(fieldName, value);
                        if (field.isPrimaryKey()) newRdns.addValue(fieldName, value);
                    }
                }

                Collection rdns = TransformEngine.convert(newRdns);

                for (Iterator j=rdns.iterator(); j.hasNext(); ) {
                    RDN rdn = (RDN)j.next();
                    DN newDn = new DN(rdn);

                    if (debug) {
                        log.debug("Adding "+snapshot.getName()+": "+newDn);
                        newAttributes.print();
                    }

                    snapshot.add(newDn, newAttributes);
                }

                rb.clear();
            }

            interpreter.clear();
        }

        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Source cache = (Source)caches.get(name);
            Source snapshot = (Source)snapshots.get(name);

            try {
                cache.drop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            snapshot.rename(cache);
        }
/*
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
*/
        log.debug("Cache synchronization completed.");
    }

    public void add(Entry entry) throws Exception {
        if (log.isDebugEnabled()) log.debug("Adding "+entry.getDn()+" to cache.");
        //cache.add(entry.getDn(), entry.getAttributes());
    }

    public void delete(Entry entry) throws Exception {
        if (log.isDebugEnabled()) log.debug("Deleting "+entry.getDn()+" from cache.");
        //cache.delete(entry.getDn());
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

        //cache.modify(cacheEntry.getDn(), modifications);
    }

    public boolean isSorted() {
        return sorted;
    }

    public void setSorted(boolean sorted) {
        this.sorted = sorted;
    }
}
