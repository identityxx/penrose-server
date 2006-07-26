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
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.server.config.PenroseServerConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.util.ExceptionUtil;
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

    PenroseServer penroseServer;
    Penrose penrose;
    PartitionManager partitionManager;

    DirectoryServiceConfiguration factoryCfg;

    boolean allowAnonymousAccess;

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;

        PenroseServerConfig penroseServerConfig = penroseServer.getPenroseServerConfig();
        ServiceConfig serviceConfig = penroseServerConfig.getServiceConfig("LDAP");
        String s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
        allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();

        Penrose penrose = penroseServer.getPenrose();
        this.partitionManager = penrose.getPartitionManager();
    }

    public void init(DirectoryServiceConfiguration factoryCfg, InterceptorConfiguration cfg) throws NamingException
    {
        super.init(factoryCfg, cfg);
        this.factoryCfg = factoryCfg;
    }

    public void bind(
            NextInterceptor next,
            Name bindDn,
            byte[] credentials,
            List mechanisms,
            String saslAuthId) throws NamingException {

        //log.debug("Binding as \""+bindDn+"\"");
        //log.debug(" - mechanisms: "+mechanisms);
        //log.debug(" - sslAuthId: "+saslAuthId);

        String password = new String((byte[])credentials);
/*

        try {
            PenroseConfig penroseConfig = penrose.getPenroseConfig();
            String rootDn = penroseConfig.getRootUserConfig().getDn();

            if (bindDn.equals(rootDn)) {
                next.bind(bindDn, credentials, mechanisms, saslAuthId);

            } else {
                PenroseSession session = penrose.newSession();
                if (session == null) throw new ServiceUnavailableException();

                int rc = session.bind(bindDn.toString(), password);
                session.close();

                if (rc != LDAPException.SUCCESS) {
                    throw new LdapAuthenticationException();
                }

                log.info("Login success.");
                next.bind(bindDn, credentials, mechanisms, saslAuthId);
            }

        } catch (NamingException e) {
            log.info("Login failed.");
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
*/
        next.bind(bindDn, credentials, mechanisms, saslAuthId);
    }

    public void unbind(NextInterceptor nextInterceptor, Name bindDn) throws NamingException {
        log.debug("Unbinding as \""+bindDn+"\"");
        nextInterceptor.unbind(bindDn);
    }

    public void add(
            NextInterceptor next,
            String upName,
            Name normName,
            Attributes attributes)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = normName.toString();

            log.debug("===============================================================================");
            log.debug("add(\""+dn+"\") as "+principalDn);

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                log.debug(dn+" is a static entry");
                next.add(upName, normName, attributes);
                return;
            }

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

            int rc = session.add(upName, attributes);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
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
            Name name,
            String attributeName,
            Object value)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

            int rc = session.compare(dn, attributeName, value);

            session.close();

            if (rc != LDAPException.COMPARE_TRUE && rc != LDAPException.COMPARE_FALSE) {
                ExceptionUtil.throwNamingException(rc);
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
            Name name)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

            int rc = session.delete(dn);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public Name getMatchedName( NextInterceptor next, Name dn, boolean normalized ) throws NamingException
    {
        log.debug("===============================================================================");
        log.debug("getMatchedName(\""+dn+"\")");
        return next.getMatchedName( dn, normalized );
    }

    public Attributes getRootDSE( NextInterceptor next ) throws NamingException
    {
        log.debug("===============================================================================");
        log.debug("getRootDSE()");
        return next.getRootDSE();
    }

    public Name getSuffix( NextInterceptor next, Name dn, boolean normalized ) throws NamingException
    {
        log.debug("===============================================================================");
        log.debug("getSuffix(\""+dn+"\")");
        return next.getSuffix( dn, normalized );
    }

    public boolean isSuffix( NextInterceptor next, Name name ) throws NamingException
    {
        log.debug("===============================================================================");
        log.debug("isSuffix(\""+name+"\")");
        return next.isSuffix( name );
    }

    public Iterator listSuffixes( NextInterceptor next, boolean normalized ) throws NamingException
    {
        log.debug("===============================================================================");
        log.debug("listSuffixes()");
        return next.listSuffixes( normalized );
    }

    public NamingEnumeration list(
            NextInterceptor next,
            Name name)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

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
            Name name)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            Partition partition = partitionManager.getPartitionByDn(dn);
            if (partition == null) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            //log.debug("===============================================================================");
            //log.debug("hasEntry(\""+dn+"\") as "+principalDn);

            EntryMapping ed = partition.findEntryMapping(dn);
            if (ed == null) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            log.debug("searching \""+dn+"\"");

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

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
            Name name,
            String[] attrIds)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();
        try {
            String dn = name.toString();

            //log.debug("===============================================================================");
            //log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+") as \""+principalDn+"\"");

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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

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
                ExceptionUtil.throwNamingException(rc);
            }

            SearchResult result = (SearchResult)results.next();

            if (result == null) {
                ExceptionUtil.throwNamingException(LDAPException.NO_SUCH_OBJECT);
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
            Name name)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            //log.debug("===============================================================================");
            //log.debug("lookup(\""+dn+"\") as \""+principalDn+"\"");

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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

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
                ExceptionUtil.throwNamingException(rc);
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
            Name base,
            Map env,
            ExprNode filter,
            SearchControls searchControls)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();
        LdapContext ctx = getContext();
        Hashtable environment = ctx.getEnvironment();
        //entryCache.setNextInterceptor(next);
        //entryCache.setContext(getContext());

        try {
            String baseDn = base.toString();

            log.debug("===============================================================================");
            log.debug("search(\""+baseDn+"\") as "+principalDn);

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

                    PenroseSession session = null;
                    try {
                        Penrose penrose = penroseServer.getPenrose();
                        session = penrose.newSession();
                        if (session == null) throw new ServiceUnavailableException();

                        if (principalDn == null) {
                            if (!allowAnonymousAccess) {
                                ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                            }
                        } else {
                            session.setBindDn(principalDn.toString());
                        }

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

                        session.close();

                        PenroseSearchResults results2 = new PenroseSearchResults();
                        results2.add(entry);
                        results2.close();

                        return new PenroseEnumeration(results2);

                    } finally {
                        if (session != null) try { session.close(); } catch (Exception e) {}
                    }
                }
            }

            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            String newFilter = FilterTool.convert(filter).toString();

            log.debug("Searching \""+base+"\"");
            log.debug(" - deref: "+deref);
            log.debug(" - scope: "+scope);
            log.debug(" - filter: "+newFilter);
            log.debug(" - attributeNames: "+attributeNames);

            PenroseSession session = null;
            try {
                Penrose penrose = penroseServer.getPenrose();
                session = penrose.newSession();
                if (session == null) throw new ServiceUnavailableException();

                if (principalDn == null) {
                    if (!allowAnonymousAccess) {
                        ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                    }
                } else {
                    session.setBindDn(principalDn.toString());
                }

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

            } finally {
                if (session != null) try { session.close(); } catch (Exception e) {}
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
            Name name,
            int modOp,
            Attributes attributes)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

            int rc = session.modify(dn.toString(), modifications);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
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
            Name name,
            ModificationItem[] modificationItems)
            throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

            int rc = session.modify(dn.toString(), Arrays.asList(modificationItems));

            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
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
            Name name,
            String newDn,
            boolean deleteOldDn
            ) throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
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

            Penrose penrose = penroseServer.getPenrose();
            PenroseSession session = penrose.newSession();
            if (session == null) throw new ServiceUnavailableException();

            if (principalDn == null) {
                if (!allowAnonymousAccess) {
                    ExceptionUtil.throwNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                }
            } else {
                session.setBindDn(principalDn.toString());
            }

            int rc = session.modrdn(dn.toString(), newDn);

            session.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }

        } catch (NamingException e) {
            throw e;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void move( NextInterceptor next, Name oriChildName, Name newParentName, String newRn, boolean deleteOldRn ) throws NamingException
    {
        String dn = oriChildName.toString();
        log.debug("===============================================================================");
        log.debug("move(\""+dn+"\")");
        next.move( oriChildName, newParentName, newRn, deleteOldRn );
    }

    public void move( NextInterceptor next, Name oriChildName, Name newParentName ) throws NamingException
    {
        String dn = oriChildName.toString();
        log.debug("===============================================================================");
        log.debug("move(\""+dn+"\")");
        next.move( oriChildName, newParentName );
    }
}
