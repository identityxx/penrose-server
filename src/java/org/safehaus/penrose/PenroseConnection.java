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
package org.safehaus.penrose;

import org.ietf.ldap.LDAPEntry;

import java.util.Date;
import java.util.List;
import java.util.Collection;
import java.io.Serializable;

/**
 * Represent an LDAP Connection made by each client
 * 
 * @author Endi S. Dewata
 */
public class PenroseConnection implements Serializable {

    private Penrose penrose;

    /**
     * Bind DN
     */
    private String bindDn;
    
    /**
     * The time and date when the connection was made
     */
    private Date date;

    public PenroseConnection(Penrose penrose) {
        this.penrose = penrose;
    }

    public String getBindDn() {
        return bindDn;
    }

    public void setBindDn(String bindDn) {
        this.bindDn = bindDn;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public int add(LDAPEntry entry) throws Exception {
        return penrose.add(this, entry);
    }

    public int bind(String dn, String password) throws Exception {
        return penrose.bind(this, dn, password);
    }

    public int compare(String dn, String attributeName, String attributeValue) throws Exception {
        return penrose.compare(this, dn, attributeName, attributeValue);
     }

    public int delete(String dn) throws Exception {
        return penrose.delete(this, dn);
    }

    public int modify(String dn, List modifications) throws Exception {
        return penrose.modify(this, dn, modifications);
    }

    public int modrdn(String dn, String newRdn) throws Exception {
        return penrose.modrdn(this, dn, newRdn);
    }

    public SearchResults search(
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames)
            throws Exception {

        return penrose.search(this, base, scope, deref, filter, attributeNames);
    }

    public int unbind() throws Exception {
        return penrose.unbind(this);
    }

    public void close() throws Exception {
        penrose.removeConnection(this);
    }
}