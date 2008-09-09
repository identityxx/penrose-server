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

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ACLEvaluator {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Partition partition;

    public ACLEvaluator() {
    }

    public void init(Partition partition) throws Exception {
        this.partition = partition;
    }

    public void addPermission(Set<String> set, String permission) {
        for (int i=0; i<permission.length(); i++) {
            set.add(permission.substring(i, i+1));
        }
    }

    public void addPermission(ACI aci, Set<String> grants, Set<String> denies) {
        if (aci.getAction().equals(ACI.ACTION_GRANT)) {
            addPermission(grants, aci.getPermission());

        } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
            addPermission(denies, aci.getPermission());
        }
    }

    public boolean getObjectPermission(
            DN bindDn,
            Entry entry,
            DN targetDn,
            String scope,
            String permission) throws Exception {

        if (entry == null) return true;

        if (debug) {
            log.debug("Checking ACL on \""+entry.getDn()+"\".");
            //log.debug("Bind DN: "+bindDn);
            //log.debug("Target DN: "+targetDn);
        }

        for (ACI aci : entry.getACL()) {
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

            String subject = aci.getSubject();
            //log.debug("   ==> checking subject "+subject);

            if (subject.equals(ACI.SUBJECT_USER)) {
                DN dn = aci.getDn();
                if (dn == null) throw new Exception("Missing dn in ACI");
                boolean match = dn.matches(bindDn);
                //log.debug("User matches \""+aci.getDn()+"\": "+match);
                if (match) {
                    return aci.getAction().equals(ACI.ACTION_GRANT);
                }
            }

            if (subject.equals(ACI.SUBJECT_SELF)) {
                boolean match = targetDn.matches(bindDn);
                //log.debug("User matches \""+targetDn+"\": "+match);
                if (match) {
                    return aci.getAction().equals(ACI.ACTION_GRANT);
                }
            }

            if (subject.equals(ACI.SUBJECT_ANONYMOUS)) {
                boolean anonymous = bindDn == null;
                //log.debug("User is anonymous: "+anonymous);
                if (anonymous) {
                    return aci.getAction().equals(ACI.ACTION_GRANT);
                }
            }

            if (subject.equals(ACI.SUBJECT_AUTHENTICATED)) {
                boolean authenticated = bindDn != null;
                //log.debug("User is authenticated: "+authenticated);
                if (authenticated) {
                    return aci.getAction().equals(ACI.ACTION_GRANT);
                }
            }

            if (subject.equals(ACI.SUBJECT_ANYBODY)) {
                return aci.getAction().equals(ACI.ACTION_GRANT);
            }
        }

        Entry parent = entry.getParent();
        if (parent == null) {
        	if (debug) {
        		log.debug("Parent entry for "+ entry.getDn()+" not found.");
        	}
            return false;
        }

        return getObjectPermission(bindDn, parent, targetDn, ACI.SCOPE_SUBTREE, permission);
    }

    public int checkPermission(
            Session session,
            Entry entry,
            DN dn,
            String permission
    ) throws Exception {
    	
        if (debug) log.debug("Checking object \""+permission+"\" permission");

        int rc = LDAP.SUCCESS;
        if (session == null) {
            log.debug("No session => SUCCESS");
            return rc;
        }

        if (session.isRootUser()) {
            log.debug("Root user => SUCCESS");
            return rc;
        }

        DN bindDn = session.getBindDn();
        boolean result = getObjectPermission(bindDn, entry, dn, ACI.SCOPE_OBJECT, permission);

        if (result) {
            log.debug("ACL evaluation => SUCCESS");
            return rc;
        }

        log.debug("ACL evaluation => FAILED");
        rc = LDAP.INSUFFICIENT_ACCESS_RIGHTS;
        return rc;
    }

    public int checkRead(Session session, Entry entry, DN dn) throws Exception {
    	return checkPermission(session, entry, dn, ACI.PERMISSION_READ);
    }

    public int checkSearch(Session session, Entry entry, DN dn) throws Exception {
    	return checkPermission(session, entry, dn, ACI.PERMISSION_SEARCH);
    }

    public int checkAdd(Session session, Entry entry, DN dn) throws Exception {
    	return checkPermission(session, entry, dn, ACI.PERMISSION_ADD);
    }

    public int checkDelete(Session session, Entry entry, DN dn) throws Exception {
    	return checkPermission(session, entry, dn, ACI.PERMISSION_DELETE);
    }

    public int checkWrite(Session session, Entry entry, DN dn) throws Exception {
    	return checkPermission(session, entry, dn, ACI.PERMISSION_WRITE);
    }

    public Collection<String> getAttributes(String attributes) {
        Collection<String> list = new ArrayList<String>();
        addAttributes(list, attributes);
        return list;
    }

    public void addAttributes(Collection<String> list, String attributes) {
        //log.debug("Adding attributes: "+attributes);
        StringTokenizer st = new StringTokenizer(attributes, ",");
        while (st.hasMoreTokens()) {
            String attributeName = st.nextToken().trim().toLowerCase();
            list.add(attributeName);
            //log.debug("Adding attribute: "+attributeName);
        }
    }

    public void addAttributes(ACI aci, Collection<String> grants, Collection<String> denies) {
        Collection<String> attributes = getAttributes(aci.getAttributes());

        if (aci.getAction().equals(ACI.ACTION_GRANT)) {
            attributes.removeAll(denies);
            grants.addAll(attributes);

        } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
            attributes.removeAll(grants);
            denies.addAll(attributes);
        }
    }

    public boolean checkSubject(DN bindDn, DN targetDn, ACI aci) throws Exception {

        String subject = aci.getSubject();
        if (debug) log.debug("   Checking bind DN ["+bindDn+"] with "+subject);

        if (subject.equals(ACI.SUBJECT_USER)) {

            DN dn = aci.getDn();
            if (dn == null) throw new Exception("Missing dn in ACI");
            if (debug) log.debug("   Comparing with ["+dn+"]");

            if (dn.matches(bindDn)) {
                log.debug("   ==> matching user");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_SELF) ) {

        	if (debug) log.debug("   Comparing with ["+targetDn+"]");

            if (targetDn.matches(bindDn)) {
                log.debug("   ==> matching self");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_ANONYMOUS)) {

            if (bindDn == null) {
                log.debug("   ==> matching anonymous");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_AUTHENTICATED)) {

            if (bindDn != null) {
                log.debug("   ==> matching authenticated");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_ANYBODY)) {
            log.debug("   ==> matching anybody");
            return true;
        }

        return false;
    }

    public void getReadableAttributes(
            DN bindDn,
            Entry entry,
            DN targetDn,
            String scope,
            Collection<String> attributeNames,
            Collection<String> grants,
            Collection<String> denies
    ) throws Exception {

        if (entry == null) {
            //log.debug("ERROR: Entry is null.");
            return;
        }

        Entry parent = entry.getParent();
        getReadableAttributes(bindDn, parent, targetDn, ACI.SCOPE_SUBTREE, attributeNames, grants, denies);

        if (debug) log.debug("Checking ACL in "+entry.getDn()+":");

        List<ACI> acls = new ArrayList<ACI>();
        for (ACI aci : entry.getACL()) {
            acls.add(0, aci);
        }

        for (ACI aci : acls) {
            if (debug) log.debug(" - " + aci);

            if (!checkSubject(bindDn, targetDn, aci)) {
                if (debug) log.debug("   ==> subject " + bindDn + " doesn't match");
                continue;
            }

            if (scope != null && !scope.equals(aci.getScope())) {
                if (debug) log.debug("   ==> scope " + scope + " doesn't match " + aci.getScope());
                continue;
            }

            if (aci.getPermission().indexOf(ACI.PERMISSION_READ) < 0) {
                if (debug) log.debug("   ==> read permission not defined");
                continue;
            }

            if (aci.getTarget().equals(ACI.TARGET_ATTRIBUTES)) {
                Collection<String> attributes = getAttributes(aci.getAttributes());

                if (aci.getAction().equals(ACI.ACTION_GRANT)) {
                    if (debug) log.debug("   ==> Granting read access to attributes " + attributes);
                    grants.addAll(attributes);
                    denies.removeAll(attributes);

                } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
                    if (debug) log.debug("   ==> Denying read access to attributes " + attributes);
                    grants.removeAll(attributes);
                    denies.addAll(attributes);
                }

                continue;
            }

            if (aci.getTarget().equals(ACI.TARGET_OBJECT)) {
                if (aci.getAction().equals(ACI.ACTION_GRANT)) {
                    if (debug) log.debug("   ==> Granting read access to attributes " + attributeNames);
                    grants.addAll(attributeNames);
                    denies.removeAll(attributeNames);

                } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
                    if (debug) log.debug("   ==> Denying read access to attributes " + attributeNames);
                    grants.removeAll(attributeNames);
                    denies.addAll(attributeNames);
                }
            }

        }
    }

    public void filterAttributes(
            Session session,
            SearchResult result
    ) throws Exception {

        if (session == null || session.isRootUser()) {
            return;
        }

        DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        Collection<String> attributeNames = attributes.getNormalizedNames();

        Set<String> grants = new HashSet<String>();
        Set<String> denies = new HashSet<String>();
        denies.addAll(attributeNames);

        DN bindDn = session.getBindDn();

        String entryId = result.getEntryId();
        Entry entry = partition.getDirectory().getEntry(entryId);

        getReadableAttributes(bindDn, entry, dn, null, attributeNames, grants, denies);

        if (debug) {
            log.debug("Returned: "+attributeNames);
            log.debug("Granted: "+grants);
            log.debug("Denied: "+denies);
        }

        for (String attributeName : attributes.getNames()) {
            String normalizedName = attributeName.toLowerCase();

            if (!denies.contains(normalizedName)) {
                //log.debug("Keep undenied attribute "+normalizedName);
                continue;
            }

            //log.debug("Remove denied attribute "+normalizedName);
            attributes.remove(attributeName);
        }

        if (debug) {
            log.debug("Returning "+dn+":");
            attributes.print();
        }
    }
}
