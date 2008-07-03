/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchResponse extends Response implements Cloneable {

    protected long bufferSize;
    protected long sizeLimit;
    protected long totalCount;
    protected boolean closed = false;

    protected boolean eventsEnabled = true;

    protected LinkedList<SearchResult> results = new LinkedList<SearchResult>();
    protected LinkedList<SearchReference> references = new LinkedList<SearchReference>();

    protected transient List<SearchListener> listeners = new ArrayList<SearchListener>();

    public SearchResponse() {
    }

    public void addListener(SearchListener listener) {
        if (listeners != null) listeners.add(0, listener);
    }

    public void removeListener(SearchListener listener) {
        if (listeners != null) listeners.remove(listener);
    }

    public synchronized void add(SearchResult result) throws Exception {

        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            exception = LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
            throw exception;
        }

        while (!closed && bufferSize > 0 && results.size() >= bufferSize) {
            Logger log = LoggerFactory.getLogger(getClass());
            try {
                log.debug("Buffer full (size: "+bufferSize+").");
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        if (listeners != null && !listeners.isEmpty()) {
            for (SearchListener listener : listeners) {
                listener.add(result);
            }
        } else {
            results.add(result);
        }

        totalCount++;

        notifyAll();
    }

    public synchronized boolean hasNext() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!closed && results.size() == 0 && references.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        if (results.size() == 0 && references.size() == 0 && exception.getResultCode() != LDAP.SUCCESS) throw exception;

        return results.size() > 0 || references.size() > 0;
    }

    public synchronized SearchResult next() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!closed && results.size() == 0 && references.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }

        if (results.size() != 0) {
            SearchResult object = results.removeFirst();

            notifyAll();

            return object;

        } else if (references.size() != 0) {

            SearchReference reference = references.removeFirst();

            notifyAll();

            throw new SearchReferenceException(reference);
        }

        return null;
    }

    public synchronized Collection<SearchResult> getAll() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!closed) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return results;
    }

    public synchronized void add(SearchReference reference) throws Exception {

        if (listeners != null && !listeners.isEmpty()) {
            for (SearchListener listener : listeners) {
                listener.add(reference);
            }
        } else {
            references.add(reference);
        }

        notifyAll();
    }

    public synchronized Collection<SearchReference> getReferences() {
        return references;
    }

    public synchronized int waitFor() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!closed) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return super.waitFor();
    }

    public synchronized void close() throws Exception {

        if (listeners != null && !listeners.isEmpty()) {
            for (SearchListener listener : listeners) {
                listener.close();
            }
        }

        closed = true;

        notifyAll();
    }

    public boolean isClosed() {
        return closed;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setSizeLimit(long sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public long getSizeLimit() {
        return sizeLimit;
    }

    public boolean isEventsEnabled() {
        return eventsEnabled;
    }

    public void setEventsEnabled(boolean eventsEnabled) {
        this.eventsEnabled = eventsEnabled;
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void copy(SearchResponse response) {
        super.copy(response);

        bufferSize = response.bufferSize;
        sizeLimit = response.sizeLimit;
        totalCount = response.totalCount;
        closed = response.closed;

        results = new LinkedList<SearchResult>();
        results.addAll(response.results);

        references = new LinkedList<SearchReference>();
        references.addAll(response.references);

        listeners = new ArrayList<SearchListener>();
    }

    public Object clone() throws CloneNotSupportedException {
        SearchResponse response = (SearchResponse)super.clone();
        response.copy(this);
        return response;
    }
}
