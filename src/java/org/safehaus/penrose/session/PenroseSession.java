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
import org.safehaus.penrose.util.EntryUtil;

import javax.naming.directory.Attributes;
import java.util.Date;
import java.util.Collection;
import java.util.Arrays;

/**
 * Represent an LDAP Connection made by each client
 * 
 * @author Endi S. Dewata
 */
public class PenroseSession {

    private Handler handler;

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

    public void setHandler(Handler handler) {
        this.handler = handler;
    }

    public int add(String dn, Attributes attributes) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.add(this, dn, attributes);
    }

    public int bind(String dn, String password) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.bind(this, dn, password);
    }

    public int compare(String dn, String attributeName, Object attributeValue) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.compare(this, dn, attributeName, attributeValue);
     }

    public int delete(String dn) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.delete(this, dn);
    }

    public int modify(String dn, Collection modifications) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.modify(this, dn, modifications);
    }

    public int modrdn(String dn, String newRdn) throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.modrdn(this, dn, newRdn);
    }

    public PenroseSearchResults search(
            String base,
            String filter,
            PenroseSearchControls sc)
            throws Exception {

        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.search(this, base, filter, sc);
    }

    public int unbind() throws Exception {
        lastActivityDate.setTime(System.currentTimeMillis());
        return handler.unbind(this);
    }

    public void close() throws Exception {
        handler.closeSession(this);
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