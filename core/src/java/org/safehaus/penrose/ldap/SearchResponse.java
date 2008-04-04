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

    protected LinkedList<SearchResult> buffer = new LinkedList<SearchResult>();

    protected long bufferSize;
    protected long sizeLimit;
    protected long totalCount;
    protected boolean closed = false;

    protected boolean eventsEnabled = true;
    protected transient List<SearchResponseListener> listeners = new ArrayList<SearchResponseListener>();

    protected LinkedList<SearchResult> referrals = new LinkedList<SearchResult>();
    protected transient Collection<ReferralListener> referralListeners = new ArrayList<ReferralListener>();

    public SearchResponse() {
    }

    public void addListener(SearchResponseListener listener) {
        if (listeners != null) listeners.add(0, listener);
    }

    public void removeListener(SearchResponseListener listener) {
        if (listeners != null) listeners.remove(listener);
    }

    public boolean firePreAddEvent(final SearchResponseEvent event) throws Exception {
        if (listeners != null) {
            for (SearchResponseListener listener : listeners) {
                boolean result = listener.preAdd(event);
                if (!result) return false;
            }
        }

        return true;
    }

    public void firePostAddEvent(final SearchResponseEvent event) throws Exception {
        if (listeners != null) {
            for (SearchResponseListener listener : listeners) {
                listener.postAdd(event);
            }
        }
    }

    public boolean firePreRemoveEvent(final SearchResponseEvent event) throws Exception {
        if (listeners != null) {
            for (SearchResponseListener listener : listeners) {
                boolean result = listener.preRemove(event);
                if (!result) return false;
            }
        }

        return true;
    }

    public void firePostRemoveEvent(final SearchResponseEvent event) throws Exception {
        if (listeners != null) {
            for (SearchResponseListener listener : listeners) {
                listener.postRemove(event);
            }
        }
    }

    public boolean firePreCloseEvent(final SearchResponseEvent event) throws Exception {
        if (listeners != null) {
            for (SearchResponseListener listener : listeners) {
                boolean result = listener.preClose(event);
                if (!result) return false;
            }
        }

        return true;
    }

    public void firePostCloseEvent(final SearchResponseEvent event) throws Exception {
        if (listeners != null) {
            for (SearchResponseListener listener : listeners) {
                listener.postClose(event);
            }
        }
    }

    public void addReferralListener(ReferralListener listener) {
        if (referralListeners != null) referralListeners.add(listener);
    }

    public void removeReferralListener(ReferralListener listener) {
        if (referralListeners != null) referralListeners.remove(listener);
    }

    public void fireEvent(final ReferralEvent event) {
        if (referralListeners != null) {
            for (ReferralListener listener : referralListeners) {
                switch (event.getType()) {
                    case ReferralEvent.REFERRAL_ADDED:
                        listener.referralAdded(event);
                        break;
                    case ReferralEvent.REFERRAL_REMOVED:
                        listener.referralRemoved(event);
                        break;
                }
            }
        }
    }

    public synchronized void waitFor() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!isClosed()) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public synchronized int getReturnCode() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!isClosed()) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return super.getReturnCode();
    }

    public synchronized Collection<SearchResult> getAll() {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!isClosed()) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return buffer;
    }

    public synchronized void addReferral(SearchResult referral) throws Exception {
        
        referrals.add(referral);

        if (eventsEnabled) {
            fireEvent(new ReferralEvent(ReferralEvent.REFERRAL_ADDED, referral));
        }

        notifyAll();
    }

    public synchronized List getReferrals() {
        return referrals;
    }

    public synchronized void add(SearchResult searchResult) throws Exception {

        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            exception = LDAP.createException(LDAP.SIZE_LIMIT_EXCEEDED);
            throw exception;
        }

        while (!isClosed() && bufferSize > 0 && buffer.size() >= bufferSize) {
            Logger log = LoggerFactory.getLogger(getClass());
            try {
                log.debug("Buffer full (size: "+bufferSize+").");
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        SearchResponseEvent event = null;

        if (eventsEnabled) {
            event = new SearchResponseEvent(SearchResponseEvent.ADD_EVENT, searchResult);
            if (!firePreAddEvent(event)) return;
            searchResult = (SearchResult)event.getObject();
        }

        buffer.add(searchResult);
        totalCount++;

        if (eventsEnabled) {
            firePostAddEvent(event);
        }

        notifyAll();
    }

    public void addAll(Collection<SearchResult> collection) throws Exception {
        for (SearchResult object : collection) {
            add(object);
        }
    }

    public synchronized boolean hasNext() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!isClosed() && buffer.size() == 0 && referrals.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        if (buffer.size() == 0 && referrals.size() == 0 && exception.getResultCode() != LDAP.SUCCESS) throw exception;

        return buffer.size() > 0 || referrals.size() > 0;
    }

    public synchronized SearchResult next() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());
        while (!isClosed() && buffer.size() == 0 && referrals.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }

        if (buffer.size() != 0) {
            SearchResult object = buffer.getFirst();

            SearchResponseEvent event = null;
            if (eventsEnabled) {
                event = new SearchResponseEvent(SearchResponseEvent.REMOVE_EVENT, object);
                if (!firePreRemoveEvent(event)) return null;
                object = (SearchResult)event.getObject();
            }

            buffer.removeFirst();

            if (eventsEnabled) {
                firePostRemoveEvent(event);
            }

            notifyAll();

            return object;

        } else if (referrals.size() != 0) {

            SearchResult reference = referrals.removeFirst();

            notifyAll();

            throw new SearchReferenceException(reference);
        }

        return null;
    }

    public synchronized void close() throws Exception {
        SearchResponseEvent event = null;
        if (eventsEnabled) {
            event = new SearchResponseEvent(SearchResponseEvent.CLOSE_EVENT);
            if (!firePreCloseEvent(event)) return;
        }

        closed = true;

        if (eventsEnabled) {
            firePostCloseEvent(event);
        }

        notifyAll();
    }

    public boolean isClosed() {
        return closed;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(long sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public boolean isEventsEnabled() {
        return eventsEnabled;
    }

    public void setEventsEnabled(boolean eventsEnabled) {
        this.eventsEnabled = eventsEnabled;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void copy(SearchResponse response) {
        super.copy(response);

        buffer = new LinkedList<SearchResult>();
        buffer.addAll(response.buffer);

        bufferSize = response.bufferSize;
        sizeLimit = response.sizeLimit;
        totalCount = response.totalCount;
        closed = response.closed;

        eventsEnabled = response.eventsEnabled;
        if (response.listeners != null) {
            listeners = new ArrayList<SearchResponseListener>();
            listeners.addAll(response.listeners);
        }

        referrals = new LinkedList<SearchResult>();
        referrals.addAll(response.referrals);

        if (response.referralListeners != null) {
            referralListeners = new ArrayList<ReferralListener>();
            referralListeners.addAll(response.referralListeners);
        }
    }

    public Object clone() throws CloneNotSupportedException {
        SearchResponse response = (SearchResponse)super.clone();
        response.copy(this);
        return response;
    }
}
