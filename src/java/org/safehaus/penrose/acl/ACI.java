/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.acl;

/**
 * @author Endi S. Dewata
 */
public class ACI {

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
    private String dn;
    private String target      = TARGET_OBJECT;
    private String attributes;
    private String scope       = SCOPE_SUBTREE;
    private String action      = ACTION_GRANT;
    private String permission;

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

    public String toString() {
        return "ACI ["+subject+(SUBJECT_USER.equals(subject) || SUBJECT_GROUP.equals(subject) ? dn : "")+"] "
                +" ["+target+(TARGET_OBJECT.equals(target) ? "" : ":"+attributes)+"] "
                +scope+" "+action+" "+permission;
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

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }
}
