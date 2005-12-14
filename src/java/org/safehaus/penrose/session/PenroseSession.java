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

import org.ietf.ldap.LDAPEntry;
import org.safehaus.penrose.handler.SessionHandler;

import java.util.Date;
import java.util.Collection;
import java.util.Arrays;

/**
 * Represent an LDAP Connection made by each client
 * 
 * @author Endi S. Dewata
 */
public class PenroseSession {

    private SessionHandler sessionHandler;

    private String sessionId;

    private String bindDn;
    
    private Date createDate;
    private Date lastActivityDate;

    public PenroseSession() {
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

    public void setHandler(SessionHandler sessionHandler) {
        this.sessionHandler = sessionHandler;
    }

    public int add(LDAPEntry entry) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.add(this, entry);
    }

    public int bind(String dn, String password) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.bind(this, dn, password);
    }

    public int compare(String dn, String attributeName, String attributeValue) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.compare(this, dn, attributeName, attributeValue);
     }

    public int delete(String dn) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.delete(this, dn);
    }

    public int modify(String dn, Collection modifications) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.modify(this, dn, modifications);
    }

    public int modrdn(String dn, String newRdn) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.modrdn(this, dn, newRdn);
    }

    public PenroseSearchResults search(
            String base,
            String filter,
            PenroseSearchControls sc)
            throws Exception {

        lastActivityDate.setTime(System.currentTimeMillis());

        int scope = sc.getScope();
        int deref = sc.getDereference();
        Collection attributes = sc.getAttributes() == null ? null : Arrays.asList(sc.getAttributes());

        return sessionHandler.search(this, base, scope, deref, filter, attributes);
    }

    public int unbind() throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return sessionHandler.unbind(this);
    }

    public void close() throws Exception {
        sessionHandler.closeSession(this);
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
}