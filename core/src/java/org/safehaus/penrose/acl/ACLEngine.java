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

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.util.EntryUtil;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ACLEngine {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private SchemaManager schemaManager;
    private PartitionManager partitionManager;

    public ACLEngine() {
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

        if (entryMapping == null) return true;

        String mappingDn = entryMapping.getDn();
        //log.debug("Checking ACL on \""+entryMapping.getDn()+"\".");
        //log.debug("Bind DN: "+bindDn);
        //log.debug("Target DN: "+targetDn);

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

            String subject = aci.getSubject();
            //log.debug("   ==> checking subject "+subject);

            if (subject.equals(ACI.SUBJECT_USER)) {
                String dn = schemaManager.normalize(aci.getDn());
                boolean match = dn.equals(bindDn);
                //log.debug("User matches \""+aci.getDn()+"\": "+match);
                if (match) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("User access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_SELF)) {
                boolean match = targetDn.equals(bindDn);
                //log.debug("User matches \""+targetDn+"\": "+match);
                if (match) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("Self access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_ANONYMOUS)) {
                boolean anonymous = bindDn == null || bindDn.equals("");
                //log.debug("User is anonymous: "+anonymous);
                if (anonymous) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("Anonymous access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_AUTHENTICATED)) {
                boolean authenticated = bindDn != null && !bindDn.equals("");
                //log.debug("User is authenticated: "+authenticated);
                if (authenticated) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("Authenticated access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_ANYBODY)) {
                boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                //log.debug("Anybody access: "+b);
                return b;
            }
        }

        Partition partition = partitionManager.getPartitionByDn(targetDn);
        if (partition == null) {
            log.debug("Partition for "+targetDn+" not found.");
            return false;
        }

        entryMapping = partition.getParent(entryMapping);
        if (entryMapping == null) {
            log.debug("Parent entry for "+mappingDn+" not found.");
            return false;
        }

        return getObjectPermission(bindDn, targetDn, entryMapping, ACI.SCOPE_SUBTREE, permission);
    }

    public int checkPermission(PenroseSession session, String dn, EntryMapping entryMapping, String permission) throws Exception {
    	
        log.debug("Checking object \""+permission+"\" permission");

        int rc = LDAPException.SUCCESS;
        if (session == null) {
            log.debug("No session => SUCCESS");
            return rc;
        }

        String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
        String bindDn = schemaManager.normalize(session.getBindDn());

        if (rootDn != null && rootDn.equals(bindDn)) {
            log.debug("Root user => SUCCESS");
            return rc;
        }

        String targetDn = schemaManager.normalize(dn);
        boolean result = getObjectPermission(bindDn, targetDn, entryMapping, ACI.SCOPE_OBJECT, permission);

        if (result) {
            log.debug("ACL evaluation => SUCCESS");
            return rc;
        }

        log.debug("ACL evaluation => FAILED");
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

    public Collection getAttributes(String attributes) {
        Collection list = new ArrayList();
        addAttributes(list, attributes);
        return list;
    }

    public void addAttributes(Collection list, String attributes) {
        //log.debug("Adding attributes: "+attributes);
        StringTokenizer st = new StringTokenizer(attributes, ",");
        while (st.hasMoreTokens()) {
            String attributeName = st.nextToken().trim();
            list.add(attributeName);
            //log.debug("Adding attribute: "+attributeName);
        }
    }

    public void addAttributes(ACI aci, Collection grants, Collection denies) {
        Collection attributes = getAttributes(aci.getAttributes());

        if (aci.getAction().equals(ACI.ACTION_GRANT)) {
            attributes.removeAll(denies);
            grants.addAll(attributes);

        } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
            attributes.removeAll(grants);
            denies.addAll(attributes);
        }
    }

    public boolean checkSubject(String bindDn, String targetDn, ACI aci) throws Exception {

        String subject = aci.getSubject();
        log.debug("   Checking bind DN ["+bindDn+"] with "+subject);

        if (subject.equals(ACI.SUBJECT_USER)) {

            String dn = schemaManager.normalize(aci.getDn());
            log.debug("   Comparing with ["+dn+"]");

            if (dn.equals(bindDn)) {
                log.debug("   ==> matching user");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_SELF) ) {

            log.debug("   Comparing with ["+targetDn+"]");

            if (targetDn.equals(bindDn)) {
                log.debug("   ==> matching self");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_ANONYMOUS)) {

            if (bindDn == null || bindDn.equals("")) {
                log.debug("   ==> matching anonymous");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_AUTHENTICATED)) {

            if (bindDn != null && !bindDn.equals("")) {
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
            String bindDn,
            String targetDn,
            EntryMapping entryMapping,
            String scope,
            Collection attributeNames,
            Collection grants,
            Collection denies) throws Exception {

        if (entryMapping == null) {
            grants.addAll(attributeNames);
            return;
        }

        log.debug("Checking ACL in "+entryMapping.getDn()+":");

        for (Iterator i=entryMapping.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            log.debug(" - "+aci);

            if (!checkSubject(bindDn, targetDn, aci)) {
                log.debug("   ==> subject doesn't match");
                continue;
            }

            if (scope != null && !scope.equals(aci.getScope())) {
                log.debug("   ==> scope doesn't match");
                continue;
            }

            if (aci.getPermission().indexOf(ACI.PERMISSION_READ) < 0) {
                log.debug("   ==> read permission not defined");
                continue;
            }

            if (aci.getTarget().equals(ACI.TARGET_ATTRIBUTES)) {
                Collection attributes = getAttributes(aci.getAttributes());

                if (aci.getAction().equals(ACI.ACTION_GRANT)) {
                    attributes.removeAll(denies);
                    log.debug("   ==> Granting read access to attributes "+attributes);
                    grants.addAll(attributes);

                } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
                    attributes.removeAll(grants);
                    log.debug("   ==> Denying read access to attributes "+attributes);
                    denies.addAll(attributes);
                }

                continue;
            }

            if (aci.getTarget().equals(ACI.TARGET_OBJECT)) {
                if (aci.getAction().equals(ACI.ACTION_GRANT)) {
                    attributeNames.removeAll(denies);
                    log.debug("   ==> Granting read access to attributes "+attributeNames);
                    grants.addAll(attributeNames);

                } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
                    attributeNames.removeAll(grants);
                    log.debug("   ==> Denying read access to attributes "+attributeNames);
                    denies.addAll(attributeNames);
                }

                continue;
            }

        }

        Partition partition = partitionManager.getPartitionByDn(entryMapping.getDn());
        if (partition == null) return;

        entryMapping = partition.getParent(entryMapping);
        if (entryMapping == null) return;

        getReadableAttributes(bindDn, targetDn, entryMapping, ACI.SCOPE_SUBTREE, attributeNames, grants, denies);
    }

    public void getReadableAttributes(
            PenroseSession session,
            String targetDn,
            EntryMapping entryMapping,
            Collection attributeNames,
            Collection grants,
            Collection denies
            ) throws Exception {

        log.debug("Checking readable attributes");

        if (session == null) {
            log.debug("No session => SUCCESS");
            grants.addAll(attributeNames);
            return;
        }

        String rootDn = schemaManager.normalize(penroseConfig.getRootDn());
        String bindDn = schemaManager.normalize(session.getBindDn());

    	if (rootDn != null && rootDn.equals(bindDn)) {
            log.debug("Root user => SUCCESS");
            grants.addAll(attributeNames);
            return;
        }

        getReadableAttributes(bindDn, targetDn, entryMapping, null, attributeNames, grants, denies);

        attributeNames.removeAll(grants);
        denies.addAll(attributeNames);
/*
        grants.removeAll(denies);
        denies.removeAll(grants);

        if (denies.contains("*")) {
            grants.clear();
            denies.clear();
            denies.add("*");
        }
*/

        log.debug("Granted: "+grants);
        log.debug("Denied: "+denies);
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }
}
