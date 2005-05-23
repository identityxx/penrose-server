/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose;

import org.ietf.ldap.LDAPEntry;
import org.safehaus.penrose.event.Event;

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