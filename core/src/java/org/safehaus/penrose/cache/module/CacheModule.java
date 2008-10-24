package org.safehaus.penrose.cache.module;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.cache.CacheKey;
import org.safehaus.penrose.cache.CacheManager;
import org.safehaus.penrose.cache.CacheMBean;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleChain;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.operation.PipelineSearchOperation;

/**
 * @author Endi Sukma Dewata
 */
public class CacheModule extends Module implements CacheMBean {

    public final static String QUERY_SIZE  = "querySize";
    public final static String RESULT_SIZE = "resultSize";
    public final static String EXPIRATION  = "expiration"; // minutes

    protected CacheManager cacheManager;

    public void init() throws Exception {

        cacheManager = new CacheManager();

        String s = getParameter(QUERY_SIZE);
        if (s != null) {
            if (debug) log.debug("Query size: "+s);
            cacheManager.setQuerySize(Integer.parseInt(s));
        }

        s = getParameter(RESULT_SIZE);
        if (s != null) {
            if (debug) log.debug("Result size: "+s);
            cacheManager.setResultSize(Integer.parseInt(s));
        }

        s = getParameter(EXPIRATION);
        if (s != null) {
            if (debug) log.debug("Expiration: "+s);
            cacheManager.setExpiration(Integer.parseInt(s));
        }
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response,
            ModuleChain chain
    ) throws Exception {

        clear();
        chain.add(session, request, response);
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response,
            ModuleChain chain
    ) throws Exception {

        String entryId = chain.getEntry().getId();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(request.getDn());
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        CacheKey key = new CacheKey();
        key.setBindDn(request.getDn());
        key.setRequest(searchRequest);
        key.setEntryId(entryId);

        Cache c = cacheManager.get(key);

        if (c != null) {
            if (warn) log.warn("Cache found for "+entryId+" "+request.getDn()+" "+searchRequest.getFilter()+".");

        } else {
            if (warn) log.warn("Cache not found for "+entryId+" "+request.getDn()+" "+searchRequest.getFilter()+".");
        }

        chain.bind(session, request, response);
    }

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response,
            ModuleChain chain
    ) throws Exception {

        String entryId = chain.getEntry().getId();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(request.getDn());
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        CacheKey key = new CacheKey();
        key.setBindDn(session.getBindDn());
        key.setRequest(searchRequest);
        key.setEntryId(entryId);

        Cache c = cacheManager.get(key);

        if (c != null) {
            if (warn) log.warn("Cache found for "+entryId+" "+request.getDn()+" "+searchRequest.getFilter()+".");

        } else {
            if (warn) log.warn("Cache not found for "+entryId+" "+request.getDn()+" "+searchRequest.getFilter()+".");
        }

        chain.compare(session, request, response);
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response,
            ModuleChain chain
    ) throws Exception {

        clear();
        chain.delete(session, request, response);
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response,
            ModuleChain chain
    ) throws Exception {

        clear();
        chain.modify(session, request, response);
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response,
            ModuleChain chain
    ) throws Exception {

        clear();
        chain.modrdn(session, request, response);
    }

    public void search(
            final SearchOperation operation,
            final ModuleChain chain
    ) throws Exception {

        String entryId = chain.getEntry().getId();

        final CacheKey key = new CacheKey();
        key.setBindDn(operation.getSession().getBindDn());
        key.setRequest((SearchRequest)operation.getRequest());
        key.setEntryId(entryId);

        Cache c = cacheManager.get(key);

        if (c != null) {
            if (warn) log.warn("Cache found for "+entryId+" "+operation.getDn()+" "+operation.getFilter()+".");

            SearchResponse sr = (SearchResponse)c.getResponse().clone();
            if (debug) log.debug("Cache contains "+sr.getTotalCount()+" entries.");

            while (sr.hasNext()) {
                try {
                    SearchResult result = sr.next();
                    operation.add(result);

                } catch (SearchReferenceException e) {
                    SearchReference reference = e.getReference();
                    operation.add(reference);
                }
            }

            operation.setException(sr.getException());

            return;
        }

        if (warn) log.warn("Cache not found for "+entryId+" "+operation.getDn()+" "+operation.getFilter()+".");

        final Cache cache = cacheManager.create(key);
        final SearchResponse cacheResponse = new SearchResponse();

        SearchOperation op = new PipelineSearchOperation(operation) {
            public void add(SearchResult result) throws Exception {
                try {
                    super.add(result);
                    cacheResponse.add(result);
                } catch (LDAPException e) {
                    cacheResponse.setException(e);
                }
            }
            public void add(SearchReference reference) throws Exception {
                super.add(reference);
                cacheResponse.add(reference);
            }
            public void close() throws Exception {
                if (debug) log.debug("Closing search response.");
                cacheResponse.close();
                cache.setResponse(cacheResponse);
                cacheManager.add(cache);
                super.close();
            }
        };

        chain.search(op);
    }

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response,
            ModuleChain chain
    ) throws Exception {

        chain.unbind(session, request, response);
    }

    public void clear() {
        cacheManager.clear();
        if (warn) log.warn("Cache cleared.");
    }
}
