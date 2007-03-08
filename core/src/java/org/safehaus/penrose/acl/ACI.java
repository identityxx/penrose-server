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
package org.safehaus.penrose.acl;

import org.safehaus.penrose.entry.DN;

/**
 * @author Endi S. Dewata
 */
public class ACI implements Cloneable {

    public final static String SUBJECT_USER          = "user";
    public final static String SUBJECT_GROUP         = "group";
    public final static String SUBJECT_SELF          = "self";
    public final static String SUBJECT_ANYBODY       = "anybody";
    public final static String SUBJECT_ANONYMOUS     = "anonymous";
    public final static String SUBJECT_AUTHENTICATED = "authenticated";

    public final static String TARGET_OBJECT         = "OBJECT";
    public final static String TARGET_ATTRIBUTES     = "ATTRIBUTES";

    public final static String SCOPE_OBJECT          = "OBJECT";
    public final static String SCOPE_SUBTREE         = "SUBTREE";

    public final static String ACTION_GRANT          = "grant";
    public final static String ACTION_DENY           = "deny";

    public final static String PERMISSION_READ       = "r";
    public final static String PERMISSION_WRITE      = "w";
    public final static String PERMISSION_SEARCH     = "s";
    public final static String PERMISSION_ADD        = "a";
    public final static String PERMISSION_DELETE     = "d";


    private String subject     = SUBJECT_ANYBODY;
    private DN dn;
    private String target      = TARGET_OBJECT;
    private String attributes;
    private String scope       = SCOPE_SUBTREE;
    private String action      = ACTION_GRANT;
    private String permission;

    public ACI() {
    }

    public ACI(String permission) {
        this.permission = permission;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getAttributes() {
        return attributes;
    }

    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Object clone() {
        ACI aci = new ACI();
        aci.subject    = subject;
        aci.dn         = dn;
        aci.target     = target;
        aci.attributes = attributes;
        aci.scope      = scope;
        aci.action     = action;
        aci.permission = permission;
        return aci;
    }

    public int hashCode() {
        return (subject == null ? 0 : subject.hashCode()) +
                (dn == null ? 0 : dn.hashCode()) +
                (target == null ? 0 : target.hashCode()) +
                (attributes == null ? 0 : attributes.hashCode()) +
                (scope == null ? 0 : scope.hashCode()) +
                (action == null ? 0 : action.hashCode()) +
                (permission == null ? 0 : permission.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        ACI aci = (ACI)object;
        if (!equals(subject, aci.subject)) return false;
        if (!equals(dn, aci.dn)) return false;
        if (!equals(target, aci.target)) return false;
        if (!equals(attributes, aci.attributes)) return false;
        if (!equals(scope, aci.scope)) return false;
        if (!equals(action, aci.action)) return false;
        if (!equals(permission, aci.permission)) return false;

        return true;
    }

    public String toString() {
        return "ACI ["+subject+(SUBJECT_USER.equals(subject) || SUBJECT_GROUP.equals(subject) ? " "+dn : "")+"] "
                +"["+target+(TARGET_OBJECT.equals(target) ? "" : ":"+attributes)+"] "
                +scope+" "+action+" "+permission;
    }

}
