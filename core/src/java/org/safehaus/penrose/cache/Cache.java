package org.safehaus.penrose.cache;

import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class Cache {

    private CacheKey key;

    private Date creationDate;
    private Date expirationDate;

    private SearchResponse response;

    public Cache() {
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public synchronized SearchResponse getResponse() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (response == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                Penrose.errorLog.error(e.getMessage(), e);
            }
        }
        return response;
    }

    public synchronized void setResponse(SearchResponse response) {
        this.response = response;
        notifyAll();
    }

    public CacheKey getKey() {
        return key;
    }

    public void setKey(CacheKey key) {
        this.key = key;
    }

    public Date getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }
}
