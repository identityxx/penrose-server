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

import org.apache.directory.server.core.interceptor.BaseInterceptor;
import org.apache.directory.server.core.interceptor.NextInterceptor;
import org.apache.directory.server.core.configuration.InterceptorConfiguration;
import org.apache.directory.server.core.DirectoryServiceConfiguration;
import org.apache.directory.shared.ldap.filter.ExprNode;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.*;
import javax.naming.ldap.LdapContext;
import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseInterceptor extends BaseInterceptor {

    public Logger log = LoggerFactory.getLogger(getClass());

    Penrose penrose;
    PartitionManager partitionManager;

    DirectoryServiceConfiguration factoryCfg;

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
        this.partitionManager = penrose.getPartitionManager();
    }

    public void init(DirectoryServiceConfiguration factoryCfg, InterceptorConfiguration cfg) throws NamingException {
        super.init(factoryCfg, cfg);
        this.factoryCfg = factoryCfg;
    }

    public PenroseSession getSession() throws Exception {

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        PenroseSession session = penrose.getSession(bindDn);

        if (session == null) {
            session = penrose.createSession(bindDn);
            if (session == null) throw new ServiceUnavailableException();
        }

        return session;
    }

    public void removeSession() throws Exception {

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        penrose.removeSession(bindDn);
    }

    public void bind(
            NextInterceptor next,
            LdapDN bindDn,
            byte[] credentials,
            List mechanisms,
            String saslAuthId
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = bindDn.toString();
            log.debug("bind(\""+dn+"\")");
            //log.debug(" - mechanisms: "+mechanisms);
            //log.debug(" - sslAuthId: "+saslAuthId);

            String password = new String((byte[])credentials);
            //log.debug(" - password: "+password);

            next.bind(bindDn, credentials, mechanisms, saslAuthId);

            PenroseSession session = getSession();
            session.setBindDn(bindDn.toString());
            session.setBindPassword(password);

            //log.debug("Bind successful.");

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void unbind(NextInterceptor nextInterceptor, LdapDN bindDn) throws NamingException {
        log.debug("===============================================================================");
        log.debug("unbind(\""+bindDn+"\")");
        nextInterceptor.unbind(bindDn);
    }

    public void add(
            NextInterceptor next,
            LdapDN name,
            Attributes attributes
    ) throws NamingException {

        log.debug("===============================================================================");

        try {
            String dn = name.getUpName();
            log.debug("add(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                next.add(name, attributes);
                return;
            }

            PenroseSession session = getSession();

            int rc = session.add(dn, attributes);

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public boolean compare(
            NextInterceptor next,
            LdapDN name,
            String attributeName,
            Object value
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("compare(\""+dn+"\", \""+attributeName+"\", "+value+")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                return next.compare(name, attributeName, value);
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.compare(name, attributeName, value);
            }

            PenroseSession session = getSession();

            int rc = session.compare(dn, attributeName, value);

            if (rc != LDAPException.COMPARE_TRUE && rc != LDAPException.COMPARE_FALSE) {
                throw ExceptionTool.createNamingException(rc);
            }

            return rc == LDAPException.COMPARE_TRUE;

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void delete(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("delete(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                next.delete(name);
                return;
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.delete(name);
                return;
            }

            PenroseSession session = getSession();

            int rc = session.delete(dn);

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public LdapDN getMatchedName(NextInterceptor next, LdapDN dn) throws NamingException {
        log.debug("===============================================================================");
        log.debug("getMatchedName(\""+dn+"\")");
        return next.getMatchedName( dn );
    }

    public Attributes getRootDSE(NextInterceptor next) throws NamingException {
        log.debug("===============================================================================");
        log.debug("getRootDSE()");
        return next.getRootDSE();
    }

    public LdapDN getSuffix(NextInterceptor next, LdapDN dn) throws NamingException {
        log.debug("===============================================================================");
        log.debug("getSuffix(\""+dn+"\")");
        return next.getSuffix( dn );
    }

    public boolean isSuffix(NextInterceptor next, LdapDN name) throws NamingException {
        log.debug("===============================================================================");
        log.debug("isSuffix(\""+name+"\")");
        return next.isSuffix( name );
    }

    public Iterator listSuffixes(NextInterceptor next) throws NamingException {
        log.debug("===============================================================================");
        log.debug("listSuffixes()");
        return next.listSuffixes( );
    }

    public NamingEnumeration list(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("list(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                return next.list(name);
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.list(name);
            }

            PenroseSession session = getSession();

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
/*
            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }
*/
            return new PenroseEnumeration(results);

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public boolean hasEntry(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("hasEntry(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            log.debug("searching \""+dn+"\"");

            PenroseSession session = getSession();

            PenroseSearchResults results = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setScope(PenroseSearchControls.SCOPE_BASE);
            sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);

            String base = name.getUpName();
            session.search(
                    base,
                    "(objectClass=*)",
                    sc,
                    results);

            boolean result = results.getReturnCode() == LDAPException.SUCCESS && results.size() == 1;

            return result;

        } catch (NamingException e) {
            //log.error(e.getMessage(), e);
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public Attributes lookup(
            NextInterceptor next,
            LdapDN name,
            String[] attrIds
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                //log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                //log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            PenroseSession session = getSession();

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

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
            }

            SearchResult result = (SearchResult)results.next();

            if (result == null) {
                throw ExceptionTool.createNamingException(LDAPException.NO_SUCH_OBJECT);
            }

            return result.getAttributes();

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public Attributes lookup(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("lookup(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                //log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                //log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            PenroseSession session = getSession();

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

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
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

    public NamingEnumeration search(
            NextInterceptor next,
            LdapDN base,
            Map env,
            ExprNode filter,
            SearchControls searchControls
    ) throws NamingException {

        log.debug("===============================================================================");

        try {
            String baseDn = base.getUpName();
            log.debug("search(\""+baseDn+"\")");

            if (!"".equals(baseDn)) {
                Partition partition = partitionManager.getPartitionByDn(baseDn);
                if (partition == null) {
                    log.debug(baseDn+" is a static entry");
                    return next.search(base, env, filter, searchControls);
                }
            }

            if (searchControls != null && searchControls.getReturningAttributes() != null) {

                if ("".equals(baseDn) && searchControls.getSearchScope() == SearchControls.OBJECT_SCOPE) {

                    NamingEnumeration ne = next.search(base, env, filter, searchControls);
                    SearchResult sr = (SearchResult)ne.next();
                    Attributes attributes = sr.getAttributes();

                    PenroseSession session = getSession();
                    PenroseSearchResults results = new PenroseSearchResults();

                    PenroseSearchControls sc = new PenroseSearchControls();
                    sc.setScope(PenroseSearchControls.SCOPE_BASE);
                    sc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
                    sc.setAttributes(searchControls == null ? null : searchControls.getReturningAttributes());

                    session.search(
                            baseDn,
                            "(objectClass=*)",
                            sc,
                            results);

                    SearchResult entry = (SearchResult)results.next();
                    Attributes set = entry.getAttributes();

                    for (NamingEnumeration ne2=attributes.getAll(); ne2.hasMore(); ) {
                        Attribute attribute = (Attribute)ne2.next();
                        String name = attribute.getID();
                        if (name.equals("vendorName") || name.equals("vendorVersion")) continue;

                        Attribute ldapAttribute = set.get(name);
                        if (ldapAttribute == null) {
                            ldapAttribute = new BasicAttribute(name);
                            set.put(ldapAttribute);
                        }

                        for (NamingEnumeration ne3=attribute.getAll(); ne3.hasMore(); ) {
                            Object value = ne3.next();
                            ldapAttribute.add(value);
                        }
                    }

                    PenroseSearchResults results2 = new PenroseSearchResults();
                    results2.add(entry);
                    results2.close();

                    return new PenroseEnumeration(results2);
                }
            }

            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            String newFilter = org.safehaus.penrose.ldap.FilterTool.convert(filter).toString();

            log.debug("Searching \""+base+"\"");
            log.debug(" - deref: "+deref);
            log.debug(" - scope: "+scope);
            log.debug(" - filter: "+newFilter);
            log.debug(" - attributeNames: "+attributeNames);

            PenroseSession session = getSession();

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

            LdapContext ctx = getContext();
            Hashtable environment = ctx.getEnvironment();
            return new PenroseEnumeration(environment, results);

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(
            NextInterceptor next,
            LdapDN name,
            int modOp,
            Attributes attributes
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("modify(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modOp, attributes);
                return;
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modOp, attributes);
                return;
            }

            Collection modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();

                ModificationItem modification = new ModificationItem(modOp, attribute);
                modifications.add(modification);
            }

            PenroseSession session = getSession();

            int rc = session.modify(dn.toString(), modifications);

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modify(
            NextInterceptor next,
            LdapDN name,
            ModificationItem[] modificationItems
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("modify(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modificationItems);
                return;
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modificationItems);
                return;
            }

            PenroseSession session = getSession();

            int rc = session.modify(dn.toString(), Arrays.asList(modificationItems));

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void modifyRn(
            NextInterceptor next,
            LdapDN name,
            String newDn,
            boolean deleteOldDn
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            String dn = name.getUpName();
            log.debug("modifyDn(\""+dn+"\")");

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                next.modifyRn(name, newDn, deleteOldDn);
                return;
            }

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.modifyRn(name, newDn, deleteOldDn);
                return;
            }

            PenroseSession session = getSession();

            int rc = session.modrdn(dn.toString(), newDn, deleteOldDn);

            if (rc != LDAPException.SUCCESS) {
                throw ExceptionTool.createNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void move(
            NextInterceptor next,
            LdapDN oriChildName,
            LdapDN newParentName,
            String newRn,
            boolean deleteOldRn
    ) throws NamingException {

        log.debug("===============================================================================");
        String dn = oriChildName.getUpName();
        log.debug("move(\""+dn+"\")");
        next.move( oriChildName, newParentName, newRn, deleteOldRn );
    }

    public void move(
            NextInterceptor next,
            LdapDN oriChildName,
            LdapDN newParentName
    ) throws NamingException {

        log.debug("===============================================================================");
        String dn = oriChildName.getUpName();
        log.debug("move(\""+dn+"\")");
        next.move( oriChildName, newParentName );
    }

}
