package org.safehaus.penrose.cache;

import org.safehaus.penrose.ldap.SearchResult;

import java.util.Date;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Iterator;

/**
 * @author Endi Sukma Dewata
 */
public class Cache {

    private Date createDate = new Date();

    private int size;
    private int expiration;

    private Collection<SearchResult> searchResults = new LinkedList<SearchResult>();

    public Cache() {
    }

    public void add(SearchResult searchResult) throws Exception {
        if (size != 0 && searchResults.size() >= size) return;
        searchResults.add(searchResult);
    }

    public Iterator iterator() throws Exception {
        return searchResults.iterator();
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public boolean isExpired() {
        return expiration != 0 &&
                createDate.getTime() + expiration * 60 * 1000 <= System.currentTimeMillis();
    }

    public Collection<SearchResult> getSearchResults() {
        return searchResults;
    }

    public void setSearchResults(Collection<SearchResult> searchResults) {
        this.searchResults = searchResults;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }
}
