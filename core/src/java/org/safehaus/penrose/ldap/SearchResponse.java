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

import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.ldap.SearchResponseListener;
import org.safehaus.penrose.ldap.SearchResponseEvent;
import org.safehaus.penrose.ldap.ReferralListener;
import org.safehaus.penrose.ldap.ReferralEvent;
import org.ietf.ldap.LDAPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchResponse<E> extends Response {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected LinkedList<E> buffer = new LinkedList<E>();

    protected long bufferSize;
    protected long sizeLimit;
    protected long totalCount;
    protected boolean closed = false;

    protected LDAPException exception;

    protected boolean eventsEnabled = true;
    protected List<SearchResponseListener> listeners = new ArrayList<SearchResponseListener>();

    protected List<Object> referrals = new ArrayList<Object>();
    protected Collection<ReferralListener> referralListeners = new ArrayList<ReferralListener>();

    public SearchResponse() {
    }

    public void addListener(SearchResponseListener listener) {
        listeners.add(0, listener);
    }

    public void removeListener(SearchResponseListener listener) {
        listeners.remove(listener);
    }

    public boolean firePreAddEvent(final SearchResponseEvent event) throws Exception {
        for (SearchResponseListener listener : listeners) {
            boolean result = listener.preAdd(event);
            if (!result) return false;
        }

        return true;
    }

    public void firePostAddEvent(final SearchResponseEvent event) throws Exception {
        for (SearchResponseListener listener : listeners) {
            listener.postAdd(event);
        }
    }

    public boolean firePreRemoveEvent(final SearchResponseEvent event) throws Exception {
        for (SearchResponseListener listener : listeners) {
            boolean result = listener.preRemove(event);
            if (!result) return false;
        }

        return true;
    }

    public void firePostRemoveEvent(final SearchResponseEvent event) throws Exception {
        for (SearchResponseListener listener : listeners) {
            listener.postRemove(event);
        }
    }

    public boolean firePreCloseEvent(final SearchResponseEvent event) throws Exception {
        for (SearchResponseListener listener : listeners) {
            boolean result = listener.preClose(event);
            if (!result) return false;
        }

        return true;
    }

    public void firePostCloseEvent(final SearchResponseEvent event) throws Exception {
        for (SearchResponseListener listener : listeners) {
            listener.postClose(event);
        }
    }

    public void addReferralListener(ReferralListener listener) {
        referralListeners.add(listener);
    }

    public void removeReferralListener(ReferralListener listener) {
        referralListeners.remove(listener);
    }

    public void fireEvent(final ReferralEvent event) {
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

    public synchronized int getReturnCode() {
        while (!closed) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return exception == null ? LDAPException.SUCCESS : exception.getResultCode();
    }

    public synchronized void addReferral(Object referral) {
        referrals.add(referral);
        fireEvent(new ReferralEvent(ReferralEvent.REFERRAL_ADDED, referral));
    }

    public synchronized void removeReferral(Object referral) {
        referrals.remove(referral);
        fireEvent(new ReferralEvent(ReferralEvent.REFERRAL_REMOVED, referral));
    }

    public synchronized List getReferrals() {
        return referrals;
    }

    public LDAPException getException() {
        return exception;
    }

    public void setException(LDAPException exception) {
        this.exception = exception;
    }

    public synchronized void add(E object) throws Exception {

        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            exception = ExceptionUtil.createLDAPException(LDAPException.SIZE_LIMIT_EXCEEDED);
            throw exception;
        }

        while (!closed && bufferSize > 0 && buffer.size() >= bufferSize) {
            try {
                log.debug("Buffer full (size: "+bufferSize+").");
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        SearchResponseEvent event = null;

        if (eventsEnabled) {
            event = new SearchResponseEvent(SearchResponseEvent.ADD_EVENT, object);
            if (!firePreAddEvent(event)) return;
            object = (E)event.getObject();
        }

        buffer.add(object);
        totalCount++;

        if (eventsEnabled) {
            firePostAddEvent(event);
        }

        notifyAll();
    }

    public void addAll(Collection<E> collection) throws Exception {
        for (E object : collection) {
            add(object);
        }
    }

    public synchronized boolean hasNext() throws Exception {
        while (!closed && buffer.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        if (buffer.size() == 0 && exception != null) throw exception;

        return buffer.size() > 0;
    }

    public synchronized E next() throws Exception {
        while (!closed && buffer.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            if (exception != null) throw exception;
        }

        if (buffer.size() == 0) return null;

        E object = buffer.getFirst();

        SearchResponseEvent event = null;
        if (eventsEnabled) {
            event = new SearchResponseEvent(SearchResponseEvent.REMOVE_EVENT, object);
            if (!firePreRemoveEvent(event)) return null;
            object = (E)event.getObject();
        }

        buffer.removeFirst();

        if (eventsEnabled) {
            firePostRemoveEvent(event);
        }

        notifyAll();

        return object;
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

    public long getTotalCount() throws Exception {
        return totalCount;
    }

    public void setException(Exception e) {
        exception = ExceptionUtil.createLDAPException(e);
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
}
