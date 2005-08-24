/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.acl;

import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ACLEngine {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Penrose penrose;

    public ACLEngine(Penrose penrose) {
        this.penrose = penrose;
    }

    public void add(ACI aci, Set grants, Set denies) {
        if (aci.getAction().equals("grant")) {
            add(grants, aci.getPermission());

        } else if (aci.getAction().equals("deny")) {
            add(denies, aci.getPermission());
        }
    }

    public void add(Set set, String permission) {
        for (int i=0; i<permission.length(); i++) {
            set.add(permission.substring(i, i+1));
        }
    }

    public void getObjectPermission(
            String bindDn,
            String dn,
            EntryDefinition entry,
            String scope,
            Set grants,
            Set denies) throws Exception {

        log.debug(" * "+dn+":");

        for (Iterator i=entry.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();

            if (!aci.getTarget().equals("OBJECT")) continue;
            if (scope != null && !scope.equals(aci.getScope())) continue;

            log.debug("   - "+aci);
            String subject = penrose.getSchema().normalize(aci.getSubject());

            if (subject.equals(bindDn)) {
                add(aci, grants, denies);

            } else if (subject.equals("self") && dn.equals(bindDn)) {
                add(aci, grants, denies);

            } else if (subject.equals("authenticated") && bindDn != null && !bindDn.equals("")) {
                add(aci, grants, denies);

            } else if (subject.equals("anybody")) {
                add(aci, grants, denies);

            }
        }

        int i = dn.indexOf(",");
        if (i < 0) return;

        dn = dn.substring(i+1);

        entry = entry.getParent();
        if (entry == null) return;

        getObjectPermission(bindDn, dn, entry, "SUBTREE", grants, denies);
    }

    public Set getObjectPermission(String bindDn, Entry entry, String target) throws Exception {

        log.debug("Evaluating object permission for "+bindDn);

        Set grants = new HashSet();
        Set denies = new HashSet();

        String dn = penrose.getSchema().normalize(entry.getDn());

        getObjectPermission(bindDn, dn, entry.getEntryDefinition(), null, grants, denies);

        grants.removeAll(denies);
        denies.removeAll(grants);

        log.debug("Object permission: "+grants);

        return grants;
    }

    public int checkSearch(PenroseConnection connection, Entry entry) throws Exception {
    	
    	if (connection == null) return LDAPException.SUCCESS;

        String rootDn = penrose.getSchema().normalize(penrose.getRootDn());
        String bindDn = penrose.getSchema().normalize(connection.getBindDn());
    	if (rootDn.equals(bindDn)) return LDAPException.SUCCESS;

        Set permission = getObjectPermission(bindDn, entry, "OBJECT");

        if (permission.contains("s")) return LDAPException.SUCCESS;

        return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
    }

    public void addAttributes(Set set, String attributes) {
        log.debug("Adding attributes: "+attributes);
        StringTokenizer st = new StringTokenizer(attributes, ",");
        while (st.hasMoreTokens()) {
            String attributeName = st.nextToken().trim();
            set.add(attributeName);
            log.debug("Adding attribute: "+attributeName);
        }
    }

    public void addAttributes(ACI aci, Set grants, Set denies) {
        if (aci.getAction().equals("grant")) {
            addAttributes(grants, aci.getAttributes());

        } else if (aci.getAction().equals("deny")) {
            addAttributes(denies, aci.getAttributes());
        }
    }

    public void getReadableAttributes(
            String bindDn,
            String dn,
            EntryDefinition entry,
            String scope,
            Set grants,
            Set denies) throws Exception {

        log.debug(" * "+dn+":");

        for (Iterator i=entry.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();

            if (!aci.getTarget().equals("ATTRIBUTES")) continue;
            if (scope != null && !scope.equals(aci.getScope())) continue;
            if (aci.getPermission().indexOf("r") < 0) continue;

            log.debug("   - "+aci);
            String subject = penrose.getSchema().normalize(aci.getSubject());

            if (subject.equals(bindDn)) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals("self") && dn.equals(bindDn)) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals("authenticated") && bindDn != null && !bindDn.equals("")) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals("anybody")) {
                addAttributes(aci, grants, denies);

            }
        }

        int i = dn.indexOf(",");
        if (i < 0) return;

        dn = dn.substring(i+1);

        entry = entry.getParent();
        if (entry == null) return;

        getReadableAttributes(bindDn, dn, entry, "SUBTREE", grants, denies);
    }

    public void getReadableAttributes(
            String bindDn,
            Entry entry,
            Set grants,
            Set denies
            ) throws Exception {

        String rootDn = penrose.getSchema().normalize(penrose.getRootDn());
    	if (rootDn.equals(bindDn)) {
            grants.add("*");
            return;
        }

        String dn = penrose.getSchema().normalize(entry.getDn());

        getReadableAttributes(bindDn, dn, entry.getEntryDefinition(), null, grants, denies);

        grants.removeAll(denies);
        denies.removeAll(grants);
        
        if (denies.contains("*")) {
            grants.clear();
            denies.clear();
            denies.add("*");
        }
    }

    public LDAPEntry filterAttributes(
            PenroseConnection connection,
            Entry entry,
            LDAPEntry ldapEntry)
            throws Exception {

        String bindDn = penrose.getSchema().normalize(connection.getBindDn());

        Set grants = new HashSet();
        Set denies = new HashSet();

        log.debug("Evaluating attributes read permission for "+bindDn);

        getReadableAttributes(bindDn, entry, grants, denies);

        log.debug("Readable attributes: "+grants);
        log.debug("Unreadable attributes: "+denies);

        LDAPAttributeSet attributeSet = ldapEntry.getAttributeSet();

        Collection list = new ArrayList();
        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            if (denies.contains("*") || denies.contains(attribute.getName())) list.add(attribute);
            if (grants.contains("*") || grants.contains(attribute.getName())) continue;
            list.add(attribute);
        }
        attributeSet.removeAll(list);

        return ldapEntry;
    }

}
