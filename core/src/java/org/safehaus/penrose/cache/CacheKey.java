package org.safehaus.penrose.cache;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.SearchRequest;

/**
 * @author Endi Sukma Dewata
 */
public class CacheKey {

    private DN bindDn;
    private SearchRequest request;
    private String entryId;

    public DN getBindDn() {
        return bindDn;
    }

    public void setBindDn(DN bindDn) {
        this.bindDn = bindDn;
    }

    public SearchRequest getRequest() {
        return request;
    }

    public void setRequest(SearchRequest request) {
        this.request = request;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public int hashCode() {
        return (bindDn == null ? 0 : bindDn.hashCode()) +
                (request == null ? 0 : request.hashCode()) +
                (entryId == null ? 0 : entryId.hashCode());
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        CacheKey cacheKey = (CacheKey)object;
        if (!equals(bindDn, cacheKey.bindDn)) return false;
        if (!equals(request, cacheKey.request)) return false;
        if (!equals(entryId, cacheKey.entryId)) return false;

        return true;
    }
}
