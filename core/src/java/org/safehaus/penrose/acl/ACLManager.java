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

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.ldap.DN;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ACLManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    PenroseConfig penroseConfig;
    PenroseContext penroseContext;

    private SchemaManager schemaManager;

    public ACLManager() {
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
            DN bindDn,
            Partition partition,
            EntryMapping entryMapping,
            DN targetDn,
            String scope,
            String permission) throws Exception {

        if (entryMapping == null) return true;

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
                DN dn = aci.getDn();
                if (dn == null) throw new Exception("Missing dn in ACI");
                boolean match = dn.matches(bindDn);
                //log.debug("User matches \""+aci.getDn()+"\": "+match);
                if (match) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("User access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_SELF)) {
                boolean match = targetDn.matches(bindDn);
                //log.debug("User matches \""+targetDn+"\": "+match);
                if (match) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("Self access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_ANONYMOUS)) {
                boolean anonymous = bindDn == null;
                //log.debug("User is anonymous: "+anonymous);
                if (anonymous) {
                    boolean b = aci.getAction().equals(ACI.ACTION_GRANT);
                    //log.debug("Anonymous access: "+b);
                    return b;
                }
            }

            if (subject.equals(ACI.SUBJECT_AUTHENTICATED)) {
                boolean authenticated = bindDn != null;
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

        EntryMapping parentEntryMapping = partition.getParent(entryMapping);
        if (parentEntryMapping == null) {
        	if (log.isDebugEnabled()) {
        		log.debug("Parent entry for "+entryMapping.getDn()+" not found.");
        	}
            return false;
        }

        return getObjectPermission(bindDn, partition, parentEntryMapping, targetDn, ACI.SCOPE_SUBTREE, permission);
    }

    public int checkPermission(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            String permission
    ) throws Exception {
    	
        if (log.isDebugEnabled()) log.debug("Checking object \""+permission+"\" permission");

        int rc = LDAPException.SUCCESS;
        if (session == null) {
            log.debug("No session => SUCCESS");
            return rc;
        }

        if (session.isRootUser()) {
            log.debug("Root user => SUCCESS");
            return rc;
        }

        DN bindDn = session.getBindDn();
        boolean result = getObjectPermission(bindDn, partition, entryMapping, dn, ACI.SCOPE_OBJECT, permission);

        if (result) {
            log.debug("ACL evaluation => SUCCESS");
            return rc;
        }

        log.debug("ACL evaluation => FAILED");
        rc = LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
        return rc;
    }

    public int checkRead(Session session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {
    	return checkPermission(session, partition, entryMapping, dn, ACI.PERMISSION_READ);
    }

    public int checkSearch(Session session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {
    	return checkPermission(session, partition, entryMapping, dn, ACI.PERMISSION_SEARCH);
    }

    public int checkAdd(Session session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {
    	return checkPermission(session, partition, entryMapping, dn, ACI.PERMISSION_ADD);
    }

    public int checkDelete(Session session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {
    	return checkPermission(session, partition, entryMapping, dn, ACI.PERMISSION_DELETE);
    }

    public int checkModify(Session session, Partition partition, EntryMapping entryMapping, DN dn) throws Exception {
    	return checkPermission(session, partition, entryMapping, dn, ACI.PERMISSION_WRITE);
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
            String attributeName = st.nextToken().trim().toLowerCase();
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

    public boolean checkSubject(DN bindDn, DN targetDn, ACI aci) throws Exception {

        String subject = aci.getSubject();
        if (log.isDebugEnabled()) log.debug("   Checking bind DN ["+bindDn+"] with "+subject);

        if (subject.equals(ACI.SUBJECT_USER)) {

            DN dn = aci.getDn();
            if (dn == null) throw new Exception("Missing dn in ACI");
            if (log.isDebugEnabled()) log.debug("   Comparing with ["+dn+"]");

            if (dn.matches(bindDn)) {
                log.debug("   ==> matching user");
                return true;
            }

            return false;
        }

        if (subject.equals(ACI.SUBJECT_SELF) ) {

        	if (log.isDebugEnabled()) log.debug("   Comparing with ["+targetDn+"]");

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
            Partition partition,
            EntryMapping entryMapping,
            DN targetDn,
            String scope,
            Collection attributeNames,
            Collection grants,
            Collection denies
    ) throws Exception {

        if (entryMapping == null) return;

        EntryMapping parentMapping = partition.getParent(entryMapping);
        getReadableAttributes(bindDn, partition, parentMapping, targetDn, ACI.SCOPE_SUBTREE, attributeNames, grants, denies);

        if (log.isDebugEnabled()) log.debug("Checking ACL in "+entryMapping.getDn()+":");

        List acls = new ArrayList();
        for (Iterator i=entryMapping.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            acls.add(0, aci);
        }

        for (Iterator i=acls.iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            log.debug(" - "+aci);

            if (!checkSubject(bindDn, targetDn, aci)) {
            	if (log.isDebugEnabled()) log.debug("   ==> subject "+bindDn+" doesn't match");
                continue;
            }

            if (scope != null && !scope.equals(aci.getScope())) {
            	if (log.isDebugEnabled()) log.debug("   ==> scope "+scope+" doesn't match "+aci.getScope());
                continue;
            }

            if (aci.getPermission().indexOf(ACI.PERMISSION_READ) < 0) {
            	if (log.isDebugEnabled()) log.debug("   ==> read permission not defined");
                continue;
            }

            if (aci.getTarget().equals(ACI.TARGET_ATTRIBUTES)) {
                Collection attributes = getAttributes(aci.getAttributes());

                if (aci.getAction().equals(ACI.ACTION_GRANT)) {
                	if (log.isDebugEnabled()) log.debug("   ==> Granting read access to attributes "+attributes);
                    grants.addAll(attributes);
                    denies.removeAll(attributes);

                } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
                	if (log.isDebugEnabled()) log.debug("   ==> Denying read access to attributes "+attributes);
                    grants.removeAll(attributes);
                    denies.addAll(attributes);
                }

                continue;
            }

            if (aci.getTarget().equals(ACI.TARGET_OBJECT)) {
                if (aci.getAction().equals(ACI.ACTION_GRANT)) {
                	if (log.isDebugEnabled()) log.debug("   ==> Granting read access to attributes "+attributeNames);
                    grants.addAll(attributeNames);
                    denies.removeAll(attributeNames);

                } else if (aci.getAction().equals(ACI.ACTION_DENY)) {
                	if (log.isDebugEnabled()) log.debug("   ==> Denying read access to attributes "+attributeNames);
                    grants.removeAll(attributeNames);
                    denies.addAll(attributeNames);
                }

                continue;
            }

        }
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;

        schemaManager = penroseContext.getSchemaManager();
    }
}
