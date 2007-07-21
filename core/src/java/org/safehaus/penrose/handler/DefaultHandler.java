package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Link;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.cache.CacheKey;
import org.safehaus.penrose.cache.CacheManager;
import org.ietf.ldap.LDAPException;

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

    public void init(HandlerConfig handlerConfig) throws Exception {
        super.init(handlerConfig);

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
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Attributes attributes = request.getAttributes();
        Collection<Object> values = attributes.getValues("objectClass");

        Collection<String> objectClasses = entryMapping.getObjectClasses();
        boolean childHasObjectClass = false;

        for (Iterator i = objectClasses.iterator(); !childHasObjectClass && i.hasNext(); ) {
            String oc = (String)i.next();

            for (Object value : values) {
                String objectClass = (String) value;
                if (childHasObjectClass = oc.equalsIgnoreCase(objectClass)) break;
            }
        }

        if (!childHasObjectClass) {
            throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
        }

        super.add(session, partition, entryMapping, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        super.delete(session, partition, entryMapping, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        super.modify(session, partition, entryMapping, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        super.modrdn(session, partition, entryMapping, request, response);

        if (cacheManager != null) cacheManager.clear();
    }

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        Link link = entryMapping.getLink();

        if (link != null) {

            forwardSearch(
                    link,
                    session,
                    partition,
                    baseMapping,
                    entryMapping,
                    request,
                    response
            );

            return;
        }

        performSearch(
                session,
                partition,
                baseMapping,
                entryMapping,
                request,
                response
        );

        searchChildren(
                session,
                partition,
                baseMapping,
                entryMapping,
                request,
                response
        );
    }

    public void forwardSearch(
            final Link link,
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        String partitionName = link.getPartitionName();
        DN dn = link.getDn();

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        Partition p = partitionName == null ? partition : partitionManager.getPartition(partitionName);

        Collection<EntryMapping> c = p.getMappings().getEntryMappings(dn == null ? entryMapping.getDn() : dn);

        SearchRequest newRequest = (SearchRequest)request.clone();

        for (EntryMapping em : c) {

            Handler handler = handlerManager.getHandler(p, em);

            handler.search(
                    session,
                    p,
                    em,
                    em,
                    newRequest,
                    response
            );
        }
    }

    public void performSearch(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        int scope = request.getScope();
        final Filter filter = request.getFilter();

        if (scope != SearchRequest.SCOPE_BASE
                && scope != SearchRequest.SCOPE_SUB
                && (scope != SearchRequest.SCOPE_ONE || partition.getMappings().getParent(entryMapping) != baseMapping)
        ) {
            // if not searching for base or subtree or immediate children) then skip
            return;
        }

        if (debug) {
            log.debug("Searching "+entryMapping.getDn()+" with scope "+ LDAPUtil.getScope(scope));
        }

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        if (!filterEvaluator.eval(entryMapping, filter)) { // Check LDAP filter
            if (debug) log.debug("Entry \""+entryMapping.getDn()+"\" doesn't match search filter.");
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
            cacheKey.setEntryMapping(entryMapping);
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

        String s = entryMapping.getParameter(CACHE);
        boolean cacheEnabled = cacheManager != null && (s == null || Boolean.parseBoolean(s));

        if (cacheEnabled) {

            log.debug("Storing results in cache.");

            newCache = cacheManager.create();

            s = entryMapping.getParameter(CACHE_SIZE);
            int cacheSize = s == null ? cacheManager.getSize() : Integer.parseInt(s);
            newCache.setSize(cacheSize);

            s = entryMapping.getParameter(CACHE_EXPIRATION);
            int cacheExpiration = s == null ? cacheManager.getExpiration() : Integer.parseInt(s);
            newCache.setExpiration(cacheExpiration);

            cacheManager.put(cacheKey, newCache);

        } else {
            log.debug("Cache not enabled.");
            newCache = null;
        }

        SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>() {

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
                partition,
                baseMapping,
                entryMapping,
                request,
                sr
        );
    }

    public void searchChildren(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        int scope = request.getScope();

        if (scope != SearchRequest.SCOPE_SUB &&
                (scope != SearchRequest.SCOPE_ONE || entryMapping != baseMapping)
        ) {
            // if not searching for subtree or immediate children then skip
            return;
        }

        Collection children = partition.getMappings().getChildren(entryMapping);

        SearchRequest newRequest = (SearchRequest)request.clone();
        if (scope == SearchRequest.SCOPE_ONE) {
            newRequest.setScope(SearchRequest.SCOPE_BASE);
        }

        for (Object aChildren : children) {
            EntryMapping childMapping = (EntryMapping) aChildren;

            Handler handler = handlerManager.getHandler(partition, childMapping);

            handler.search(
                    session,
                    partition,
                    baseMapping,
                    childMapping,
                    newRequest,
                    response
            );
        }
    }
}
