package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.Partitions;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.mapping.Link;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.cache.CacheKey;
import org.safehaus.penrose.cache.CacheManager;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DefaultHandler extends Handler {

    public final static String CACHE                 = "cache";
    public final static boolean DEFAULT_CACHE        = false; // disabled

    public final static String CACHE_SIZE            = "cacheSize";
    public final static int DEFAULT_CACHE_SIZE       = 10; // entries

    public final static String CACHE_EXPIRATION      = "cacheExpiration";
    public final static int DEFAULT_CACHE_EXPIRATION = 10; // minutes

    protected CacheManager cacheManager;

    public DefaultHandler() throws Exception {
    }

    public void init() throws Exception {
        super.init();

        String s = handlerConfig.getParameter(CACHE);
        boolean cacheEnabled = s == null ? DEFAULT_CACHE : Boolean.parseBoolean(s);

        s = handlerConfig.getParameter(CACHE_SIZE);
        int cacheSize = s == null ? DEFAULT_CACHE_SIZE : Integer.parseInt(s);

        s = handlerConfig.getParameter(CACHE_EXPIRATION);
        int cacheExpiration = s == null ? DEFAULT_CACHE_EXPIRATION : Integer.parseInt(s);

        if (cacheEnabled) {
            cacheManager = new CacheManager(cacheSize);
            cacheManager.setExpiration(cacheExpiration);
        }
    }

    public void add(
            Session session,
            Partition partition,
            Entry entry,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Attributes attributes = request.getAttributes();
        Collection<Object> values = attributes.getValues("objectClass");

        Collection<String> objectClasses = entry.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (Object value : values) {
                String objectClass = (String) value;
                if (childHasObjectClass = oc.equalsIgnoreCase(objectClass)) break;
            }
        }

        if (!childHasObjectClass) {
            throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
        }

        super.add(session, partition, entry, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void delete(
            Session session,
            Partition partition,
            Entry entry,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        super.delete(session, partition, entry, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void modify(
            Session session,
            Partition partition,
            Entry entry,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        super.modify(session, partition, entry, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void modrdn(
            Session session,
            Partition partition,
            Entry entry,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        super.modrdn(session, partition, entry, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void search(
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        Link link = entry.getLink();

        if (link != null) {

            forwardSearch(
                    link,
                    session,
                    base,
                    entry,
                    sourceValues,
                    request,
                    response
            );

            return;
        }

        performSearch(
                session,
                base,
                entry,
                sourceValues,
                request,
                response
        );

        searchChildren(
                session,
                base,
                entry,
                sourceValues,
                request,
                response
        );
    }

    public void forwardSearch(
            final Link link,
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        String partitionName = link.getPartitionName();
        DN dn = link.getDn();

        Directory directory = partition.getDirectory();
        Collection<Entry> c = directory.getEntries(dn == null ? entry.getDn() : dn);

        Partitions partitions = penroseContext.getPartitions();
        Partition p = partitionName == null ? partition : partitions.getPartition(partitionName);

        SearchRequest newRequest = (SearchRequest)request.clone();

        for (Entry e : c) {

            Handler handler = partitions.getHandler(p, e);

            handler.search(
                    session,
                    e,
                    e,
                    sourceValues,
                    newRequest,
                    response
            );
        }
    }

    public void performSearch(
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        int scope = request.getScope();

        if (scope != SearchRequest.SCOPE_BASE
                && scope != SearchRequest.SCOPE_SUB
                && (scope != SearchRequest.SCOPE_ONE || entry.getParent() != base)
        ) {
            // if not searching for base or subtree or immediate children) then skip
            return;
        }

        if (debug) {
            log.debug("Searching "+entry.getDn()+" with scope "+ LDAP.getScope(scope));
        }

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        final Filter filter = request.getFilter();
        if (!filterEvaluator.eval(entry, filter)) { // Check LDAP filter
            if (debug) log.debug("Entry \""+entry.getDn()+"\" doesn't match search filter.");
            return;
        }

        CacheKey cacheKey;
        Cache cache;

        if (cacheManager == null) {
            cacheKey = null;
            cache = null;

        } else {
            cacheKey = new CacheKey();
            cacheKey.setSearchRequest(request);
            cacheKey.setEntry(entry);
            cache = cacheManager.get(cacheKey);
        }

        if (cache != null) {
            log.debug("Returning results from cache.");
            for (SearchResult searchResult : cache.getSearchResults()) {
                response.add(searchResult);
            }

            return;
        }

        final Cache newCache;

        String s = entry.getParameter(CACHE);
        boolean cacheEnabled = cacheManager != null && (s == null || Boolean.parseBoolean(s));

        if (cacheEnabled) {

            log.debug("Storing results in cache.");

            newCache = cacheManager.create();

            s = entry.getParameter(CACHE_SIZE);
            int cacheSize = s == null ? cacheManager.getSize() : Integer.parseInt(s);
            newCache.setSize(cacheSize);

            s = entry.getParameter(CACHE_EXPIRATION);
            int cacheExpiration = s == null ? cacheManager.getExpiration() : Integer.parseInt(s);
            newCache.setExpiration(cacheExpiration);

            cacheManager.put(cacheKey, newCache);

        } else {
            log.debug("Cache not enabled.");
            newCache = null;
        }

        SearchResponse sr = new SearchResponse() {

            public void add(SearchResult searchResult) throws Exception {

                if (debug) log.debug("Checking filter "+filter);

                if (!filterEvaluator.eval(searchResult.getAttributes(), filter)) { // Check LDAP filter
                    if (debug) log.debug("Entry \""+searchResult.getDn()+"\" doesn't match search filter.");
                    return;
                }

                response.add(searchResult);
                if (newCache != null) newCache.add(searchResult);
            }
        };

        super.performSearch(
                session,
                base,
                entry,
                sourceValues,
                request,
                sr
        );
    }

    public void searchChildren(
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        int scope = request.getScope();

        if (scope != SearchRequest.SCOPE_SUB &&
                (scope != SearchRequest.SCOPE_ONE || entry != base)
        ) {
            // if not searching for subtree or immediate children then skip
            return;
        }

        Partitions partitions = penroseContext.getPartitions();
        Collection<Entry> children = entry.getChildren();

        SearchRequest newRequest = (SearchRequest)request.clone();
        if (scope == SearchRequest.SCOPE_ONE) {
            newRequest.setScope(SearchRequest.SCOPE_BASE);
        }

        for (Entry child : children) {

            Handler handler = partitions.getHandler(partition, child);

            handler.search(
                    session,
                    base,
                    child,
                    sourceValues,
                    newRequest,
                    response
            );
        }
    }
}
