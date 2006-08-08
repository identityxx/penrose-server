/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import javax.naming.directory.Attributes;
import java.util.Date;
import java.util.Collection;

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

    public int add(String dn, Attributes attributes) throws Exception {
        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        AddEvent beforeModifyEvent = new AddEvent(this, AddEvent.BEFORE_ADD, this, dn, attributes);
        eventManager.postEvent(dn, beforeModifyEvent);

        int rc = handler.add(this, dn, attributes);

        AddEvent afterModifyEvent = new AddEvent(this, AddEvent.AFTER_ADD, this, dn, attributes);
        afterModifyEvent.setReturnCode(rc);
        eventManager.postEvent(dn, afterModifyEvent);

        return rc;
    }

    public int bind(String dn, String password) throws Exception {
        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        BindEvent beforeBindEvent = new BindEvent(this, BindEvent.BEFORE_BIND, this, dn, password);
        eventManager.postEvent(dn, beforeBindEvent);

        int rc = handler.bind(this, dn, password);

        BindEvent afterBindEvent = new BindEvent(this, BindEvent.AFTER_BIND, this, dn, password);
        afterBindEvent.setReturnCode(rc);
        eventManager.postEvent(dn, afterBindEvent);

        return rc;
    }

    public int compare(String dn, String attributeName, Object attributeValue) throws Exception {
        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        CompareEvent beforeCompareEvent = new CompareEvent(this, CompareEvent.BEFORE_COMPARE, this, dn, attributeName, attributeValue);
        eventManager.postEvent(dn, beforeCompareEvent);

        int rc = handler.compare(this, dn, attributeName, attributeValue);

        CompareEvent afterCompareEvent = new CompareEvent(this, CompareEvent.AFTER_COMPARE, this, dn, attributeName, attributeValue);
        afterCompareEvent.setReturnCode(rc);
        eventManager.postEvent(dn, afterCompareEvent);

        return rc;
     }

    public int delete(String dn) throws Exception {
        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        DeleteEvent beforeDeleteEvent = new DeleteEvent(this, DeleteEvent.BEFORE_DELETE, this, dn);
        eventManager.postEvent(dn, beforeDeleteEvent);

        int rc = handler.delete(this, dn);

        DeleteEvent afterDeleteEvent = new DeleteEvent(this, DeleteEvent.AFTER_DELETE, this, dn);
        afterDeleteEvent.setReturnCode(rc);
        eventManager.postEvent(dn, afterDeleteEvent);

        return rc;
    }

    public int modify(String dn, Collection modifications) throws Exception {

        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        ModifyEvent beforeModifyEvent = new ModifyEvent(this, ModifyEvent.BEFORE_MODIFY, this, dn, modifications);
        eventManager.postEvent(dn, beforeModifyEvent);

        int rc = handler.modify(this, dn, modifications);

        ModifyEvent afterModifyEvent = new ModifyEvent(this, ModifyEvent.AFTER_MODIFY, this, dn, modifications);
        afterModifyEvent.setReturnCode(rc);
        eventManager.postEvent(dn, afterModifyEvent);

        return rc;
    }

    public int modrdn(String dn, String newRdn) throws Exception {

        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        ModRdnEvent beforeModRdnEvent = new ModRdnEvent(this, ModRdnEvent.BEFORE_MODRDN, this, dn, newRdn);
        eventManager.postEvent(dn, beforeModRdnEvent);

        int rc = handler.modrdn(this, dn, newRdn);

        ModRdnEvent afterModRdnEvent = new ModRdnEvent(this, ModRdnEvent.AFTER_MODRDN, this, dn, newRdn);
        afterModRdnEvent.setReturnCode(rc);
        eventManager.postEvent(dn, afterModRdnEvent);

        return rc;
    }

    /**
     * Asynchronous search.
     * @param baseDn
     * @param filter
     * @param sc
     * @param results This will be filled with objects of type SearchResult.
     * @return
     * @throws Exception
     */
    public int search(
            final String baseDn,
            final String filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results)
            throws Exception {

        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        SearchEvent beforeSearchEvent = new SearchEvent(this, SearchEvent.BEFORE_SEARCH, this, baseDn, filter, sc, results);
        eventManager.postEvent(baseDn, beforeSearchEvent);

        final PenroseSession session = this;

        results.addListener(new PipelineAdapter() {
            public void pipelineClosed(PipelineEvent event) {
                try {
                    SearchEvent afterSearchEvent = new SearchEvent(session, SearchEvent.AFTER_SEARCH, session, baseDn, filter, sc, results);
                    afterSearchEvent.setReturnCode(results.getReturnCode());
                    eventManager.postEvent(baseDn, afterSearchEvent);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        });

        return handler.search(this, baseDn, filter, sc, results);
    }

    /**
     * Synchronous search.
     * @param baseDn
     * @param filter
     * @param sc
     * @return Results will contain objects of type SearchResult.
     * @throws Exception
     */
    public PenroseSearchResults search(
            String baseDn,
            String filter,
            PenroseSearchControls sc)
            throws Exception {

        PenroseSearchResults results = new PenroseSearchResults();
        search(baseDn, filter, sc, results);
        return results;
    }

    public int unbind() throws Exception {

        if (!sessionManager.isValid(this)) throw new Exception("Invalid session.");

        lastActivityDate.setTime(System.currentTimeMillis());

        BindEvent beforeUnbindEvent = new BindEvent(this, BindEvent.BEFORE_UNBIND, this, bindDn);
        eventManager.postEvent(bindDn, beforeUnbindEvent);

        int rc = handler.unbind(this);

        BindEvent afterUnbindEvent = new BindEvent(this, BindEvent.AFTER_UNBIND, this, bindDn);
        eventManager.postEvent(bindDn, afterUnbindEvent);

        return rc;
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

    public String getBindPassword() {
        return bindPassword;
    }

    public void setBindPassword(String bindPassword) {
        this.bindPassword = bindPassword;
    }
}