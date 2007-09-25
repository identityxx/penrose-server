package org.safehaus.penrose.cache;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.ldap.SearchRequest;

/**
 * @author Endi Sukma Dewata
 */
public class CacheKey {

    private SearchRequest searchRequest;
    private Entry entry;

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }


    public int hashCode() {
        return (searchRequest == null ? 0 : searchRequest.hashCode()) +
                (entry == null ? 0 : entry.hashCode());
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
        if (!equals(entry.getId(), cacheKey.entry.getId())) return false;

        return true;
    }
}
