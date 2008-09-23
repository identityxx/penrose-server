package org.safehaus.penrose.cache.module;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.cache.CacheKey;
import org.safehaus.penrose.cache.CacheManager;
import org.safehaus.penrose.cache.CacheMBean;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleChain;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Session;

/**
 * @author Endi Sukma Dewata
 */
public class CacheModule extends Module implements CacheMBean {

    public final static String SIZE       = "size";
    public final static String EXPIRATION = "expiration"; // minutes

    protected CacheManager cacheManager;

    public void init() throws Exception {

        cacheManager = new CacheManager();

        String s = getParameter(SIZE);
        if (s != null) {
            cacheManager.setSize(Integer.parseInt(s));
        }

        s = getParameter(EXPIRATION);
        if (s != null) {
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

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(request.getDn());
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        CacheKey key = new CacheKey();
        key.setBindDn(request.getDn());
        key.setRequest(searchRequest);
        key.setEntryId(chain.getEntry().getId());

        Cache c = cacheManager.get(key);

        if (c != null) {
            if (log.isWarnEnabled()) log.warn("Cache found for "+request.getDn()+".");

        } else {
            if (log.isWarnEnabled()) log.warn("Cache not found for "+request.getDn()+".");
        }

        chain.bind(session, request, response);
    }

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response,
            ModuleChain chain
    ) throws Exception {

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setDn(request.getDn());
        searchRequest.setScope(SearchRequest.SCOPE_BASE);

        CacheKey key = new CacheKey();
        key.setBindDn(session.getBindDn());
        key.setRequest(searchRequest);
        key.setEntryId(chain.getEntry().getId());

        Cache c = cacheManager.get(key);

        if (c != null) {
            if (log.isWarnEnabled()) log.warn("Cache found for "+request.getDn()+".");

        } else {
            if (log.isWarnEnabled()) log.warn("Cache not found for "+request.getDn()+".");
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
            Session session,
            SearchRequest request,
            SearchResponse response,
            ModuleChain chain
    ) throws Exception {

        final CacheKey key = new CacheKey();
        key.setBindDn(session.getBindDn());
        key.setRequest(request);
        key.setEntryId(chain.getEntry().getId());

        Cache c = cacheManager.get(key);

        if (c != null) {
            if (warn) log.warn("Cache found for "+request.getDn()+".");

            SearchResponse sr = (SearchResponse)c.getResponse().clone();
            if (debug) log.debug("Cache contains "+sr.getTotalCount()+" entries.");

            while (sr.hasNext()) {
                try {
                    SearchResult result = sr.next();
                    response.add(result);

                } catch (SearchReferenceException e) {
                    SearchReference reference = e.getReference();
                    response.add(reference);
                }
            }

            response.setException(response.getException());

            return;
        }

        if (log.isWarnEnabled()) log.warn("Cache not found for "+request.getDn()+".");

        final Cache cache = cacheManager.create(key);
        final SearchResponse cacheResponse = new SearchResponse();

        SearchResponse sr = new Pipeline(response) {
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
                cacheResponse.close();
                cache.setResponse(cacheResponse);
            }
        };

        chain.search(session, request, sr);
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
        if (log.isWarnEnabled()) log.warn("Cache cleared.");
    }
}
