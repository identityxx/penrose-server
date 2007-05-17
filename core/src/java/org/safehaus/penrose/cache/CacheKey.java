package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.ldap.SearchRequest;

/**
 * @author Endi Sukma Dewata
 */
public class CacheKey {

    private SearchRequest searchRequest;
    private EntryMapping entryMapping;

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }


    public int hashCode() {
        return (searchRequest == null ? 0 : searchRequest.hashCode()) +
                (entryMapping == null ? 0 : entryMapping.hashCode());
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
        if (!equals(searchRequest, cacheKey.searchRequest)) return false;
        if (!equals(entryMapping.getId(), cacheKey.entryMapping.getId())) return false;

        return true;
    }
}
