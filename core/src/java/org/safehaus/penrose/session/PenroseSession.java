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

import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.Attributes;
import java.util.Date;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

/**
 * @author Endi S. Dewata
 */
public class PenroseSession {

    Logger log = LoggerFactory.getLogger(getClass());

    private SessionManager sessionManager;
    private EventManager eventManager;

    private Handler handler;

    private String sessionId;

    private String bindDn;
    private String bindPassword;

    private Date createDate;
    private Date lastActivityDate;

    private Map attributes = new HashMap();

    public PenroseSession(SessionManager sessionManager) {
        this.sessionManager = sessionManager;

        createDate = new Date();
        lastActivityDate = (Date)createDate.clone();
    }

    public String getBindDn() {
        return bindDn;
    }

    public void setBindDn(String bindDn) {
        this.bindDn = bindDn;
    }

    public Date getLastActivityDate() {
        return lastActivityDate;
    }

    public void setLastActivityDate(Date lastActivityDate) {
        this.lastActivityDate = lastActivityDate;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public boolean isValid() {
        return sessionManager.isValid(this);
    }

    public void add(String dn, Attributes attributes) throws LDAPException {
        int rc = LDAPException.SUCCESS;

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            AddEvent beforeAddEvent = new AddEvent(this, AddEvent.BEFORE_ADD, this, dn, attributes);
            boolean b = eventManager.postEvent(dn, beforeAddEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            handler.add(this, dn, attributes);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            AddEvent afterAddEvent = new AddEvent(this, AddEvent.AFTER_ADD, this, dn, attributes);
            afterAddEvent.setReturnCode(rc);
            try {
                eventManager.postEvent(dn, afterAddEvent);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public void bind(String dn, String password) throws LDAPException {
        int rc = LDAPException.SUCCESS;

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, this, dn, password);
            boolean b = eventManager.postEvent(dn, beforeBindEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            handler.bind(this, dn, password);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, this, dn, password);
            afterBindEvent.setReturnCode(rc);
            try {
                eventManager.postEvent(dn, afterBindEvent);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public boolean compare(String dn, String attributeName, Object attributeValue) throws Exception {
        int rc = LDAPException.SUCCESS;

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            CompareEvent beforeCompareEvent = new CompareEvent(this, CompareEvent.BEFORE_COMPARE, this, dn, attributeName, attributeValue);
            boolean b = eventManager.postEvent(dn, beforeCompareEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            return handler.compare(this, dn, attributeName, attributeValue);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            CompareEvent afterCompareEvent = new CompareEvent(this, CompareEvent.AFTER_COMPARE, this, dn, attributeName, attributeValue);
            afterCompareEvent.setReturnCode(rc);
            try {
                eventManager.postEvent(dn, afterCompareEvent);
            } catch (Exception e) {
                // ignore
            }
        }
     }

    public void delete(String dn) throws LDAPException {
        int rc = LDAPException.SUCCESS;

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, this, dn);
            boolean b = eventManager.postEvent(dn, beforeDeleteEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            handler.delete(this, dn);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, this, dn);
            afterDeleteEvent.setReturnCode(rc);
            try {
                eventManager.postEvent(dn, afterDeleteEvent);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public void modify(String dn, Collection modifications) throws LDAPException {

        int rc = LDAPException.SUCCESS;

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, this, dn, modifications);
            boolean b = eventManager.postEvent(dn, beforeModifyEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            handler.modify(this, dn, modifications);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, this, dn, modifications);
            afterModifyEvent.setReturnCode(rc);
            try {
                eventManager.postEvent(dn, afterModifyEvent);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public void modrdn(String dn, String newRdn, boolean deleteOldRdn) throws LDAPException {

        int rc = LDAPException.SUCCESS;

        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            ModRdnEvent beforeModRdnEvent = new ModRdnEvent(this, ModRdnEvent.BEFORE_MODRDN, this, dn, newRdn, deleteOldRdn);
            boolean b = eventManager.postEvent(dn, beforeModRdnEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            handler.modrdn(this, dn, newRdn, deleteOldRdn);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            ModRdnEvent afterModRdnEvent = new ModRdnEvent(this, ModRdnEvent.AFTER_MODRDN, this, dn, newRdn, deleteOldRdn);
            afterModRdnEvent.setReturnCode(rc);
            try {
                eventManager.postEvent(dn, afterModRdnEvent);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Asynchronous search.
     * @param baseDn
     * @param filter
     * @param sc
     * @param results This will be filled with objects of type SearchResult.
     * @return return code
     * @throws Exception
     */
    public int search(
            String baseDn,
            String filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results)
            throws Exception {

        if (!isValid()) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        SearchEvent beforeSearchEvent = new SearchEvent(this, SearchEvent.BEFORE_SEARCH, this, baseDn, filter, sc, results);
        boolean b = eventManager.postEvent(baseDn, beforeSearchEvent);

        if (!b) {
            results.close();
            return LDAPException.SUCCESS;
        }

        final String newBaseDn = beforeSearchEvent.getBaseDn();
        final String newFilter = beforeSearchEvent.getFilter();
        final PenroseSearchControls newSc = beforeSearchEvent.getSearchControls();

        final PenroseSession session = this;

        results.addListener(new PipelineAdapter() {
            public void pipelineClosed(PipelineEvent event) {
                try {
                    lastActivityDate.setTime(System.currentTimeMillis());

                    SearchEvent afterSearchEvent = new SearchEvent(session, SearchEvent.AFTER_SEARCH, session, newBaseDn, newFilter, newSc, results);
                    afterSearchEvent.setReturnCode(results.getReturnCode());
                    eventManager.postEvent(newBaseDn, afterSearchEvent);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        });

        return handler.search(this, newBaseDn, newFilter, newSc, results);
    }

    public void unbind() throws LDAPException {

        int rc = LDAPException.SUCCESS;
        try {
            if (!isValid()) throw new Exception("Invalid session.");

            lastActivityDate.setTime(System.currentTimeMillis());

            BindEvent beforeUnbindEvent = new BindEvent(this, BindEvent.BEFORE_UNBIND, this, bindDn);
            boolean b = eventManager.postEvent(bindDn, beforeUnbindEvent);

            if (!b) {
                rc = LDAPException.UNWILLING_TO_PERFORM;
                throw new LDAPException(LDAPException.resultCodeToString(rc), rc, null);
            }

            handler.unbind(this);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            throw e;

        } catch (Exception e) {
            rc = LDAPException.OPERATIONS_ERROR;
            String message = e.getMessage();
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            BindEvent afterUnbindEvent = new BindEvent(this, BindEvent.AFTER_UNBIND, this, bindDn);
            try {
                eventManager.postEvent(bindDn, afterUnbindEvent);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public void close() throws Exception {
        sessionManager.closeSession(this);
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public EventManager getEventManager() {
        return eventManager;
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }

    public void addAddListener(AddListener listener) {
        eventManager.addAddListener(listener);
    }

    public void removeAddListener(AddListener listener) {
        eventManager.removeAddListener(listener);
    }

    public void addBindListener(BindListener listener) {
        eventManager.addBindListener(listener);
    }

    public void removeBindListener(BindListener listener) {
        eventManager.removeBindListener(listener);
    }

    public void addCompareListener(CompareListener listener) {
        eventManager.addCompareListener(listener);
    }

    public void removeCompareListener(CompareListener listener) {
        eventManager.removeCompareListener(listener);
    }

    public void addDeleteListener(DeleteListener listener) {
        eventManager.addDeleteListener(listener);
    }

    public void removeDeleteListener(DeleteListener listener) {
        eventManager.removeDeleteListener(listener);
    }

    public void addModifyListener(ModifyListener listener) {
        eventManager.addModifyListener(listener);
    }

    public void removeModifyListener(ModifyListener listener) {
        eventManager.removeModifyListener(listener);
    }

    public void addModrdnListener(ModRdnListener listener) {
        eventManager.addModRdnListener(listener);
    }

    public void removeModrdnListener(ModRdnListener listener) {
        eventManager.removeModRdnListener(listener);
    }

    public void addSearchListener(SearchListener listener) {
        eventManager.addSearchListener(listener);
    }

    public void removeSearchListener(SearchListener listener) {
        eventManager.removeSearchListener(listener);
    }

    public Collection getAttributes() {
        return attributes.values();
    }

    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    public Collection getAttributeNames() {
        return attributes.keySet();
    }

    public String getBindPassword() {
        return bindPassword;
    }

    public void setBindPassword(String bindPassword) {
        this.bindPassword = bindPassword;
    }
}