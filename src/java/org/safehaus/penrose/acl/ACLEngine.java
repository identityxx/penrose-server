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
package org.safehaus.penrose.acl;

import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.handler.Handler;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ACLEngine {

    public Logger log = Logger.getLogger(getClass());

    public Handler handler;

    public ACLEngine(Handler handler) {
        this.handler = handler;
    }

    public void addPermission(Set set, String permission) {
        for (int i=0; i<permission.length(); i++) {
            set.add(permission.substring(i, i+1));
        }
    }

    public void addPermission(ACI aci, Set grants, Set denies) {
        if (aci.getAction().equals(ACI.ACTION_GRANT)) {
            addPermission(grants, aci.getPermission());

        } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
            addPermission(denies, aci.getPermission());
        }
    }

    public boolean getObjectPermission(
            String bindDn,
            String targetDn,
            EntryMapping entryMapping,
            String scope,
            String permission) throws Exception {

        targetDn = handler.getSchemaManager().normalize(targetDn);
        //log.debug(" * "+targetDn);

        for (Iterator i=entryMapping.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            //log.debug(" - "+aci);

            if (!aci.getTarget().equals(ACI.TARGET_OBJECT)) {
                //log.debug("   ==> not checking target");
                continue;
            }

            if (scope != null && !scope.equals(aci.getScope()) && !aci.getScope().equals(ACI.SCOPE_SUBTREE)) {
                //log.debug("   ==> scope "+scope+" doesn't match");
                continue;
            }

            if (aci.getPermission().indexOf(permission) < 0) {
                //log.debug("   ==> permission "+permission+" not defined");
                continue;
            }

            String subject = handler.getSchemaManager().normalize(aci.getSubject());
            //log.debug("   ==> checking subject "+subject);

            if (subject.equals(ACI.SUBJECT_USER) && aci.getDn().equals(bindDn)) {
                //log.debug("   ==> matching user");
                return aci.getAction().equals(ACI.ACTION_GRANT);

            } else if (subject.equals(ACI.SUBJECT_SELF) && targetDn.equals(bindDn)) {
                //log.debug("   ==> matching self");
                return aci.getAction().equals(ACI.ACTION_GRANT);

            } else if (subject.equals(ACI.SUBJECT_ANONYMOUS) && (bindDn == null || bindDn.equals(""))) {
                //log.debug("   ==> matching anonymous");
                return aci.getAction().equals(ACI.ACTION_GRANT);

            } else if (subject.equals(ACI.SUBJECT_AUTHENTICATED) && bindDn != null && !bindDn.equals("")) {
                //log.debug("   ==> matching authenticated");
                return aci.getAction().equals(ACI.ACTION_GRANT);

            } else if (subject.equals(ACI.SUBJECT_ANYBODY)) {
                //log.debug("   ==> matching anybody");
                return aci.getAction().equals(ACI.ACTION_GRANT);
            }
        }

        Partition partition = handler.getPartitionManager().getPartitionByDn(targetDn);
        if (partition == null) {
            //log.debug("Partition for "+dn+" not found.");
            return false;
        }

        entryMapping = partition.getParent(entryMapping);
        if (entryMapping == null) {
            //log.debug("Parent entry for "+dn+" not found.");
            return false;
        }

        return getObjectPermission(bindDn, targetDn, entryMapping, ACI.SCOPE_SUBTREE, permission);
    }

    public int checkPermission(PenroseSession session, String dn, EntryMapping entryMapping, String permission) throws Exception {
    	
        //log.debug("Evaluating object \""+permission+"\" permission on "+entryMapping.getDn()+" for "+dn+(session == null ? null : " as "+session.getBindDn()));

        int rc = LDAPException.SUCCESS;
        if (session == null) {
            //log.debug("no session => SUCCESS");
            return rc;
        }

        String rootDn = handler.getSchemaManager().normalize(handler.getRootUserConfig().getDn());
        String bindDn = handler.getSchemaManager().normalize(session.getBindDn());
        if (rootDn != null && rootDn.equals(bindDn)) {
            //log.debug("root user => SUCCESS");
            return rc;
        }

        String targetDn = handler.getSchemaManager().normalize(dn);
        boolean result = getObjectPermission(bindDn, targetDn, entryMapping, ACI.SCOPE_OBJECT, permission);

        if (result) {
            //log.debug("acl evaluation => SUCCESS");
            return rc;
        }

        log.debug("acl evaluation => FAILED");
        rc = LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
        return rc;
    }

    public int checkRead(PenroseSession session, String dn, EntryMapping entryMapping) throws Exception {
    	return checkPermission(session, dn, entryMapping, ACI.PERMISSION_READ);
    }

    public int checkSearch(PenroseSession session, String dn, EntryMapping entryMapping) throws Exception {
    	return checkPermission(session, dn, entryMapping, ACI.PERMISSION_SEARCH);
    }

    public int checkAdd(PenroseSession session, String dn, EntryMapping entryMapping) throws Exception {
    	return checkPermission(session, dn, entryMapping, ACI.PERMISSION_ADD);
    }

    public int checkDelete(PenroseSession session, String dn, EntryMapping entryMapping) throws Exception {
    	return checkPermission(session, dn, entryMapping, ACI.PERMISSION_DELETE);
    }

    public int checkModify(PenroseSession session, String dn, EntryMapping entryMapping) throws Exception {
    	return checkPermission(session, dn, entryMapping, ACI.PERMISSION_WRITE);
    }

    public void addAttributes(Set set, String attributes) {
        //log.debug("Adding attributes: "+attributes);
        StringTokenizer st = new StringTokenizer(attributes, ",");
        while (st.hasMoreTokens()) {
            String attributeName = st.nextToken().trim();
            set.add(attributeName);
            //log.debug("Adding attribute: "+attributeName);
        }
    }

    public void addAttributes(ACI aci, Set grants, Set denies) {
        if (aci.getAction().equals(ACI.ACTION_GRANT)) {
            addAttributes(grants, aci.getAttributes());

        } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
            addAttributes(denies, aci.getAttributes());
        }
    }

    public void getReadableAttributes(
            String bindDn,
            String targetDn,
            EntryMapping entry,
            String scope,
            Set grants,
            Set denies) throws Exception {

        //log.debug(" * "+entry.getDn()+":");

        for (Iterator i=entry.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();

            if (!aci.getTarget().equals(ACI.TARGET_ATTRIBUTES)) continue;
            if (scope != null && !scope.equals(aci.getScope())) continue;
            if (aci.getPermission().indexOf(ACI.PERMISSION_READ) < 0) continue;

            //log.debug("   - "+aci);
            String subject = handler.getSchemaManager().normalize(aci.getSubject());

            if (subject.equals(bindDn)) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals(ACI.SUBJECT_SELF) && targetDn.equals(bindDn)) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals(ACI.SUBJECT_ANONYMOUS) && (bindDn == null || bindDn.equals(""))) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals(ACI.SUBJECT_AUTHENTICATED) && bindDn != null && !bindDn.equals("")) {
                addAttributes(aci, grants, denies);

            } else if (subject.equals(ACI.SUBJECT_ANYBODY)) {
                addAttributes(aci, grants, denies);

            }
        }

        Partition partition = handler.getPartitionManager().getPartitionByDn(entry.getDn());
        if (partition == null) return;

        entry = partition.getParent(entry);
        if (entry == null) return;

        getReadableAttributes(bindDn, targetDn, entry, ACI.SCOPE_SUBTREE, grants, denies);
    }

    public void getReadableAttributes(
            String bindDn,
            Entry entry,
            Set grants,
            Set denies
            ) throws Exception {

        String rootDn = handler.getSchemaManager().normalize(handler.getRootUserConfig().getDn());
    	if (rootDn.equals(bindDn)) {
            grants.add("*");
            return;
        }

        String targetDn = handler.getSchemaManager().normalize(entry.getDn());

        getReadableAttributes(bindDn, targetDn, entry.getEntryMapping(), null, grants, denies);

        grants.removeAll(denies);
        denies.removeAll(grants);
        
        if (denies.contains("*")) {
            grants.clear();
            denies.clear();
            denies.add("*");
        }
    }

    public LDAPEntry filterAttributes(
            PenroseSession session,
            Entry entry,
            LDAPEntry ldapEntry)
            throws Exception {

        String bindDn = handler.getSchemaManager().normalize(session.getBindDn());

        Set grants = new HashSet();
        Set denies = new HashSet();

        //log.debug("Evaluating attributes read permission for "+bindDn);

        getReadableAttributes(bindDn, entry, grants, denies);

        //log.debug("Readable attributes: "+grants);
        //log.debug("Unreadable attributes: "+denies);

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
