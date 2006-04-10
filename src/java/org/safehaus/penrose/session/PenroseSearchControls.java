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

import org.ietf.ldap.LDAPSearchConstraints;
import org.ietf.ldap.LDAPConnection;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchControls {

    public static int SCOPE_BASE      = LDAPConnection.SCOPE_BASE;
    public static int SCOPE_ONE       = LDAPConnection.SCOPE_ONE;
    public static int SCOPE_SUB       = LDAPConnection.SCOPE_SUB;

    public static int DEREF_ALWAYS    = LDAPSearchConstraints.DEREF_ALWAYS;
    public static int DEREF_FINDING   = LDAPSearchConstraints.DEREF_FINDING;
    public static int DEREF_NEVER     = LDAPSearchConstraints.DEREF_NEVER;
    public static int DEREF_SEARCHING = LDAPSearchConstraints.DEREF_SEARCHING;

    private int scope         = SCOPE_SUB;
    private int dereference   = DEREF_ALWAYS;
    private boolean typesOnly = false;
    private String[] attributes;

    public PenroseSearchControls() {
    }

    public int getDereference() {
        return dereference;
    }

    public void setDereference(int dereference) {
        this.dereference = dereference;
    }

    public boolean isTypesOnly() {
        return typesOnly;
    }

    public void setTypesOnly(boolean typesOnly) {
        this.typesOnly = typesOnly;
    }

    public int getScope() {
        return scope;
    }

    public void setScope(int scope) {
        this.scope = scope;
    }

    public String[] getAttributes() {
        return attributes;
    }

    public void setAttributes(String[] attributes) {
        this.attributes = attributes;
    }
}
