package org.safehaus.penrose.cache;

import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Endi Sukma Dewata
 */
public class Cache {

    private Date createDate = new Date();

    private int expiration; // minutes

    private SearchResponse response;

    public Cache() {
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

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public synchronized SearchResponse getResponse() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (response == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        return response;
    }

    public synchronized void setResponse(SearchResponse response) {
        this.response = response;
        notifyAll();
    }
}
