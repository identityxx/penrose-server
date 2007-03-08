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
package org.safehaus.penrose.session;

import org.safehaus.penrose.util.ExceptionUtil;
import org.ietf.ldap.LDAPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResults extends Results {

    public Logger log = LoggerFactory.getLogger(getClass());

    LinkedList list = new LinkedList();
    long sizeLimit;
    int totalCount;
    boolean closed = false;

    LDAPException exception;

    boolean enableEventListeners = true;
    List listeners = new ArrayList();

    List referrals = new ArrayList();
    Collection referralListeners = new ArrayList();

    public PenroseSearchResults() {
    }

    public void addListener(ResultsListener listener) {
        listeners.add(0, listener);
    }

    public void removeListener(ResultsListener listener) {
        listeners.remove(listener);
    }

    public boolean firePreAddEvent(final ResultsEvent event) throws Exception {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ResultsListener listener = (ResultsListener)i.next();
            boolean result = listener.preAdd(event);
            if (!result) return false;
        }

        return true;
    }

    public void firePostAddEvent(final ResultsEvent event) throws Exception {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ResultsListener listener = (ResultsListener)i.next();
            listener.postAdd(event);
        }
    }

    public boolean firePreRemoveEvent(final ResultsEvent event) throws Exception {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ResultsListener listener = (ResultsListener)i.next();
            boolean result = listener.preRemove(event);
            if (!result) return false;
        }

        return true;
    }

    public void firePostRemoveEvent(final ResultsEvent event) throws Exception {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ResultsListener listener = (ResultsListener)i.next();
            listener.postRemove(event);
        }
    }

    public boolean firePreCloseEvent(final ResultsEvent event) throws Exception {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ResultsListener listener = (ResultsListener)i.next();
            boolean result = listener.preClose(event);
            if (!result) return false;
        }

        return true;
    }

    public void firePostCloseEvent(final ResultsEvent event) throws Exception {
        for (Iterator i=listeners.iterator(); i.hasNext(); ) {
            ResultsListener listener = (ResultsListener)i.next();
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
        for (Iterator i=referralListeners.iterator(); i.hasNext(); ) {
            ReferralListener listener = (ReferralListener)i.next();
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

    public synchronized void add(Object object) throws Exception {

        if (sizeLimit > 0 && totalCount >= sizeLimit) {
            exception = ExceptionUtil.createLDAPException(LDAPException.SIZE_LIMIT_EXCEEDED);
            throw exception;
        }

        ResultsEvent event = null;
        if (enableEventListeners) {
            event = new ResultsEvent(ResultsEvent.ADD_EVENT, object);
            if (!firePreAddEvent(event)) return;
            object = event.getObject();
        }

        list.add(object);
        totalCount++;

        if (enableEventListeners) {
            firePostAddEvent(event);
        }

        notifyAll();
    }

    public synchronized void addAll(Collection collection) throws Exception {
        ResultsEvent event = null;
        for (Iterator i=collection.iterator(); i.hasNext(); ) {
            Object object = i.next();

            if (sizeLimit > 0 && totalCount >= sizeLimit) {
                exception = ExceptionUtil.createLDAPException(LDAPException.SIZE_LIMIT_EXCEEDED);
                throw exception;
            }

            if (enableEventListeners) {
                event = new ResultsEvent(ResultsEvent.ADD_EVENT, object);
                if (!firePreAddEvent(event)) continue;
                object = event.getObject();
            }

            list.add(object);
            totalCount++;

            if (enableEventListeners) {
                firePostAddEvent(event);
            }
        }

        notifyAll();
    }

    public synchronized boolean hasNext() throws Exception {
        while (!closed && list.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        if (list.size() == 0 && exception != null) throw exception;

        return list.size() > 0;
    }

    public synchronized Object next() throws Exception {
        while (!closed && list.size() == 0) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            if (exception != null) throw exception;
        }

        if (list.size() == 0) return null;

        Object object = list.getFirst();

        ResultsEvent event = null;
        if (enableEventListeners) {
            event = new ResultsEvent(ResultsEvent.REMOVE_EVENT, object);
            if (!firePreRemoveEvent(event)) return null;
            object = event.getObject();
        }

        list.removeFirst();

        if (enableEventListeners) {
            firePostRemoveEvent(event);
        }

        return object;
    }

    public synchronized void close() throws Exception {
        ResultsEvent event = null;
        if (enableEventListeners) {
            event = new ResultsEvent(ResultsEvent.CLOSE_EVENT);
            if (!firePreCloseEvent(event)) return;
        }

        closed = true;

        if (enableEventListeners) {
            firePostCloseEvent(event);
        }

        notifyAll();
    }

    public int getTotalCount() {
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

    public boolean isEnableEventListeners() {
        return enableEventListeners;
    }

    public void setEnableEventListeners(boolean enableEventListeners) {
        this.enableEventListeners = enableEventListeners;
    }
}
