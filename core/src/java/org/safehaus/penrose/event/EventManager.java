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
package org.safehaus.penrose.event;

import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.SessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class EventManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    public Collection<AddListener> addListeners         = new ArrayList<AddListener>();
    public Collection<BindListener> bindListeners       = new ArrayList<BindListener>();
    public Collection<CompareListener> compareListeners = new ArrayList<CompareListener>();
    public Collection<DeleteListener> deleteListeners   = new ArrayList<DeleteListener>();
    public Collection<ModifyListener> modifyListeners   = new ArrayList<ModifyListener>();
    public Collection<ModRdnListener> modrdnListeners   = new ArrayList<ModRdnListener>();
    public Collection<SearchListener> searchListeners   = new ArrayList<SearchListener>();
    public Collection<UnbindListener> unbindListeners   = new ArrayList<UnbindListener>();

    public void postEvent(AddEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        AddResponse response = event.getResponse();

        Collection<AddListener> listeners = new ArrayList<AddListener>();
        listeners.addAll(addListeners);

        for (AddListener listener : listeners) {

            switch (event.getType()) {
                case AddEvent.BEFORE_ADD:
                    listener.beforeAdd(event);
                    break;

                case AddEvent.AFTER_ADD:
                    listener.afterAdd(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(BindEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        BindResponse response = event.getResponse();

        Collection<BindListener> listeners = new ArrayList<BindListener>();
        listeners.addAll(bindListeners);

        for (BindListener listener : listeners) {

            switch (event.getType()) {
                case BindEvent.BEFORE_BIND:
                    listener.beforeBind(event);
                    break;

                case BindEvent.AFTER_BIND:
                    listener.afterBind(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(CompareEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        CompareResponse response = event.getResponse();

        Collection<CompareListener> listeners = new ArrayList<CompareListener>();
        listeners.addAll(compareListeners);

        for (CompareListener listener : listeners) {

            switch (event.getType()) {
                case CompareEvent.BEFORE_COMPARE:
                    listener.beforeCompare(event);
                    break;

                case CompareEvent.AFTER_COMPARE:
                    listener.afterCompare(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(DeleteEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        DeleteResponse response = event.getResponse();

        Collection<DeleteListener> listeners = new ArrayList<DeleteListener>();
        listeners.addAll(deleteListeners);

        for (DeleteListener listener : listeners) {

            switch (event.getType()) {
                case DeleteEvent.BEFORE_DELETE:
                    listener.beforeDelete(event);
                    break;

                case DeleteEvent.AFTER_DELETE:
                    listener.afterDelete(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(ModifyEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        ModifyResponse response = event.getResponse();

        Collection<ModifyListener> listeners = new ArrayList<ModifyListener>();
        listeners.addAll(modifyListeners);

        for (ModifyListener listener : listeners) {

            switch (event.getType()) {
                case ModifyEvent.BEFORE_MODIFY:
                    listener.beforeModify(event);
                    break;

                case ModifyEvent.AFTER_MODIFY:
                    listener.afterModify(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(ModRdnEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        ModRdnResponse response = event.getResponse();

        Collection<ModRdnListener> listeners = new ArrayList<ModRdnListener>();
        listeners.addAll(modrdnListeners);

        for (ModRdnListener listener : listeners) {

            switch (event.getType()) {
                case ModRdnEvent.BEFORE_MODRDN:
                    listener.beforeModRdn(event);
                    break;

                case ModRdnEvent.AFTER_MODRDN:
                    listener.afterModRdn(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(SearchEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        SearchResponse response = event.getResponse();

        Collection<SearchListener> listeners = new ArrayList<SearchListener>();
        listeners.addAll(searchListeners);

        for (SearchListener listener : listeners) {

            switch (event.getType()) {
                case SearchEvent.BEFORE_SEARCH:
                    listener.beforeSearch(event);
                    break;

                case SearchEvent.AFTER_SEARCH:
                    listener.afterSearch(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void postEvent(UnbindEvent event) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Firing "+event+" event.");

        UnbindResponse response = event.getResponse();

        Collection<UnbindListener> listeners = new ArrayList<UnbindListener>();
        listeners.addAll(unbindListeners);

        for (UnbindListener listener : listeners) {

            switch (event.getType()) {
                case UnbindEvent.BEFORE_UNBIND:
                    listener.beforeUnbind(event);
                    break;

                case UnbindEvent.AFTER_UNBIND:
                    listener.afterUnbind(event);
                    break;
            }

            LDAPException exception = response.getException();
            if (exception.getResultCode() != LDAP.SUCCESS) throw exception;
        }
    }

    public void addAddListener(AddListener listener) {
        if (!addListeners.contains(listener)) addListeners.add(listener);
    }

    public void removeAddListener(AddListener listener) {
        addListeners.remove(listener);
    }

    public void addBindListener(BindListener listener) {
        if (!bindListeners.contains(listener)) bindListeners.add(listener);
    }

    public void removeBindListener(BindListener listener) {
        bindListeners.remove(listener);
    }

    public void addUnbindListener(UnbindListener listener) {
        if (!unbindListeners.contains(listener)) unbindListeners.add(listener);
    }

    public void removeUnbindListener(UnbindListener listener) {
        unbindListeners.remove(listener);
    }

    public void addCompareListener(CompareListener listener) {
        if (!compareListeners.contains(listener)) compareListeners.add(listener);
    }

    public void removeCompareListener(CompareListener listener) {
        compareListeners.remove(listener);
    }

    public void addDeleteListener(DeleteListener listener) {
        if (!deleteListeners.contains(listener)) deleteListeners.add(listener);
    }

    public void removeDeleteListener(DeleteListener listener) {
        deleteListeners.remove(listener);
    }

    public void addModifyListener(ModifyListener listener) {
        if (!modifyListeners.contains(listener)) modifyListeners.add(listener);
    }

    public void removeModifyListener(ModifyListener listener) {
        modifyListeners.remove(listener);
    }

    public void addModRdnListener(ModRdnListener listener) {
        if (!modrdnListeners.contains(listener)) modrdnListeners.add(listener);
    }

    public void removeModRdnListener(ModRdnListener listener) {
        modrdnListeners.remove(listener);
    }

    public void addSearchListener(SearchListener listener) {
        if (!searchListeners.contains(listener)) searchListeners.add(listener);
    }

    public void removeSearchListener(SearchListener listener) {
        searchListeners.remove(listener);
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }
}
