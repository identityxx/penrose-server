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
package org.safehaus.penrose.ldap;

import org.apache.directory.server.core.partition.AbstractDirectoryPartition;
import org.apache.directory.shared.ldap.filter.ExprNode;
import org.ietf.ldap.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.*;
import javax.naming.directory.*;
import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenrosePartition extends AbstractDirectoryPartition {

    Logger log = LoggerFactory.getLogger(getClass());

    Penrose penrose;
    PartitionManager partitionManager;

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
        this.partitionManager = penrose.getPartitionManager();
    }

    public void doInit() throws NamingException {

        String name = getConfiguration().getName();

        File dir = new File("partitions/"+name);
        if (!dir.exists()) return;

        log.debug("-------------------------------------------------------------------------------");
        log.debug("Initializing "+name+" partition ...");
    }

    public void bind(Name bindDn, byte[] credentials, List mechanisms, String saslAuthId) throws NamingException {
        log.info("Binding as \""+bindDn+"\"");
        log.info(" - mechanisms: \""+mechanisms+"\"");
        log.info(" - sslAuthId: \""+saslAuthId+"\"");
    }

    public void unbind(Name bindDn) throws NamingException {
        log.info("Unbinding as \""+bindDn+"\"");
    }

    public void delete(Name dn) throws NamingException {
        log.info("Deleting \""+dn+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.delete(dn.toString());

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void add(String upName, Name normName, Attributes attributes) throws NamingException {
        log.info("Adding \""+upName+"\"");

        if (getSuffix(true).equals(normName)) return;

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.add(upName, attributes);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(Name dn, int modOp, Attributes attributes) throws NamingException {
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            Collection modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();

                ModificationItem modification = new ModificationItem(modOp, attribute);
                modifications.add(modification);
            }

            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.modify(dn.toString(), modifications);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(Name dn, ModificationItem[] modificationItems) throws NamingException {
        log.info("Modifying \""+dn+"\"");
        log.debug("changetype: modify");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.modify(dn.toString(), Arrays.asList(modificationItems));

            session.close();

            if (rc != LDAPException.SUCCESS) {
                throw new NamingException("RC: "+rc);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public NamingEnumeration list(Name dn) throws NamingException {
        log.info("Listing \""+dn+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_ONE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String baseDn = dn.toString();
            session.search(
                    baseDn,
                    "(objectClass=*)",
                    sc,
                    results);

            return new PenroseEnumeration(results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public NamingEnumeration search(Name base, Map env, ExprNode filter, SearchControls searchControls) throws NamingException {

        PenroseSession session = null;
        try {
            String baseDn = base.toString();
            String bindDn = (String)env.get(Context.SECURITY_PRINCIPAL);
            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            String newFilter = FilterTool.convert(filter).toString();

            log.info("Searching \""+baseDn+"\" as "+bindDn);
            log.debug(" - deref: "+deref);
            log.debug(" - scope: "+scope);
            log.debug(" - filter: "+newFilter);
            log.debug(" - attributeNames: "+attributeNames);

            session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (bindDn != null) session.setBindDn(bindDn);

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(searchControls.getSearchScope());
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
            sc.setAttributes(searchControls == null ? null : searchControls.getReturningAttributes());

            session.search(
                    baseDn,
                    newFilter,
                    sc,
                    results);

            return new PenroseEnumeration(results);

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());

        } finally {
            if (session != null) try { session.close(); } catch (Exception e) {}
        }
    }

    public void throwNamingException(int rc, String message) throws NamingException {
        switch (rc) {
            case LDAPException.SUCCESS:
                break;

            case LDAPException.NO_SUCH_OBJECT:
                throw new NameNotFoundException(message);

            default:
                throw new NamingException("RC: "+rc);
        }
    }

    public Attributes lookup(Name dn) throws NamingException {
        log.debug("Looking up \""+dn+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_BASE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String baseDn = dn.toString();
            session.search(
                    baseDn,
                    "(objectClass=*)",
                    sc,
                    results);

            int rc = results.getReturnCode();
            session.close();

            if (rc != LDAPException.SUCCESS) return null;
            //throwNamingException(rc, baseDn);

            if (!results.hasNext()) {
                throw new NameNotFoundException("No such object.");
            }

            SearchResult result = (SearchResult)results.next();

            return result.getAttributes();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public Attributes lookup(Name name, String[] attrIds) throws NamingException {

        try {
            String dn = name.toString();

            //log.debug("===============================================================================");
            //log.debug("lookup(\""+dn+"\") as \""+principalDn+"\"");

            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_BASE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String baseDn = dn.toString();
            session.search(
                    baseDn,
                    "(objectClass=*)",
                    sc,
                    results);

            int rc = results.getReturnCode();
            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionTool.throwNamingException(rc);
            }

            SearchResult result = (SearchResult)results.next();

            return result.getAttributes();

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public boolean hasEntry(Name name) throws NamingException {
        log.info("Checking \""+name+"\"");

        try {
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_BASE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String base = name.toString();
            session.search(
                    base,
                    "(objectClass=*)",
                    sc,
                    results);

            boolean result = results.getReturnCode() == LDAPException.SUCCESS && results.size() == 1;

            session.close();

            return result;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modifyRn(Name name, String newRn, boolean deleteOldRn) throws NamingException {
        try {
            String dn = name.toString();

            log.debug("===============================================================================");
            log.debug("modifyDn(\""+dn+"\")");

            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            int rc = session.modrdn(dn.toString(), newRn);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionTool.throwNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void move(Name oriChildName, Name newParentName) throws NamingException {
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void move(Name oriChildName, Name newParentName, String newRn, boolean deleteOldRn) throws NamingException {
        log.info("Moving \""+oriChildName+"\" to \""+newParentName+"\"");
    }

    public void sync() throws NamingException {
        //log.info("sync();");
    }

    public void close() throws NamingException {
        try {
            penrose.stop();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public boolean isClosed() {
        //log.info("isClosed();");
        return false;
    }
}
