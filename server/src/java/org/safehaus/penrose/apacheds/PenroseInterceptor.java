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
package org.safehaus.penrose.apacheds;

import org.apache.directory.server.core.interceptor.BaseInterceptor;
import org.apache.directory.server.core.interceptor.NextInterceptor;
import org.apache.directory.server.core.configuration.InterceptorConfiguration;
import org.apache.directory.server.core.DirectoryServiceConfiguration;
import org.apache.directory.shared.ldap.filter.ExprNode;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.session.SearchResult;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.*;
import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseInterceptor extends BaseInterceptor {

    public Logger log = LoggerFactory.getLogger(getClass());

    PenroseServer penroseServer;
    PartitionManager partitionManager;

    DirectoryServiceConfiguration factoryCfg;

    boolean allowAnonymousAccess;

    public PenroseServer getPenroseServer() {
        return penroseServer;
    }

    public void setPenroseServer(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;

        PenroseConfig penroseConfig = penroseServer.getPenroseConfig();
        ServiceConfig serviceConfig = penroseConfig.getServiceConfig("LDAP");
        String s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
        allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();

        Penrose penrose = penroseServer.getPenrose();
        PenroseContext penroseContext = penrose.getPenroseContext();
        partitionManager = penroseContext.getPartitionManager();
    }

    public void init(
            DirectoryServiceConfiguration factoryCfg,
            InterceptorConfiguration cfg
    ) throws NamingException {

        super.init(factoryCfg, cfg);
        this.factoryCfg = factoryCfg;
    }

    public Session getSession() throws Exception {

        Penrose penrose = penroseServer.getPenrose();
        if (penrose == null) {
            throw new Exception("Penrose is not initialized.");
        }

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        Session session = penrose.getSession(bindDn);

        if (session == null) {
            session = penrose.createSession(bindDn);
            if (session == null) throw new ServiceUnavailableException();
        }

        return session;
    }

    public void removeSession() throws Exception {

        Penrose penrose = penroseServer.getPenrose();
        if (penrose == null) {
            throw new Exception("Penrose is not initialized.");
        }

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        penrose.removeSession(bindDn);
    }

    public void bind(
            NextInterceptor next,
            LdapDN bindDn,
            byte[] credentials,
            List mechanisms,
            String saslAuthId) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(bindDn.getUpName());
            log.debug("bind(\""+dn+"\")");
            //log.debug(" - mechanisms: "+mechanisms);
            //log.debug(" - sslAuthId: "+saslAuthId);

            String password = new String((byte[])credentials);
            //log.debug(" - password: "+password);

            next.bind(bindDn, credentials, mechanisms, saslAuthId);

            Session session = getSession();
            session.bind(dn, password);

            //log.debug("Bind successful.");

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
    }

    public void unbind(
            NextInterceptor nextInterceptor,
            LdapDN bindDn
    ) throws NamingException {

        log.debug("===============================================================================");
        log.debug("unbind(\""+bindDn+"\")");
        nextInterceptor.unbind(bindDn);
    }

    public void add(
            NextInterceptor next,
            LdapDN name,
            Attributes attrs
    ) throws NamingException {

        log.debug("===============================================================================");

        try {
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("add(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                next.add(name, attrs);
                return;
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            org.safehaus.penrose.entry.Attributes attributes = new org.safehaus.penrose.entry.Attributes();
            for (NamingEnumeration ne = attrs.getAll(); ne.hasMore(); ) {
                javax.naming.directory.Attribute attribute = (javax.naming.directory.Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration ne2 = attribute.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attributes.addValue(attributeName, value);
                }
            }

            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            session.add(request, response);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
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
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("compare(\""+dn+"\", \""+attributeName+"\", "+value+")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.compare(name, attributeName, value);
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            return session.compare(dn, attributeName, value);
        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void delete(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("delete(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                next.delete(name);
                return;
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            session.delete(dn);
        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public LdapDN getMatchedName(NextInterceptor next, LdapDN dn) throws NamingException {
        log.debug("===============================================================================");
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("getMatchedName(\""+dn+"\")");
        return next.getMatchedName(dn);
    }

    public Attributes getRootDSE(NextInterceptor next) throws NamingException {
        log.debug("===============================================================================");
        log.debug("getRootDSE()");
        return next.getRootDSE();
    }

    public LdapDN getSuffix(NextInterceptor next, LdapDN dn) throws NamingException {
        log.debug("===============================================================================");
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("getSuffix(\""+dn+"\")");
        return next.getSuffix(dn);
    }

    public boolean isSuffix(NextInterceptor next, LdapDN name) throws NamingException {
        log.debug("===============================================================================");
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("isSuffix(\""+name+"\")");
        return next.isSuffix(name);
    }

    public Iterator listSuffixes(NextInterceptor next) throws NamingException {
        log.debug("===============================================================================");
        log.debug("listSuffixes()");
        return next.listSuffixes();
    }

    public NamingEnumeration list(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("list(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.list(name);
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_ONE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            return new PenroseEnumeration(response);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public boolean hasEntry(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("hasEntry(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            if (debug) log.debug("searching \""+dn+"\"");

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            return response.hasNext();
        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public Attributes lookup(
            NextInterceptor next,
            LdapDN name,
            String[] attrIds
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(name.getUpName());
            log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+")");

            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            SearchResult result = (SearchResult)response.next();
            Entry entry = result.getEntry();

            javax.naming.directory.SearchResult sr = EntryTool.createSearchResult(entry);
            return sr.getAttributes();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public Attributes lookup(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(name.getUpName());
            log.debug("lookup(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            boolean debug = log.isDebugEnabled();
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setDereference(SearchRequest.DEREF_ALWAYS);

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            SearchResult result = (SearchResult)response.next();
            Entry entry = result.getEntry();

            javax.naming.directory.SearchResult sr = EntryTool.createSearchResult(entry);
            return sr.getAttributes();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
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
            DN baseDn = new DN(base.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) {
                StringBuffer sb = new StringBuffer();
                filter.printToBuffer(sb);
            	log.debug("search(\""+baseDn+"\")");
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            Partition partition = partitionManager.getPartition(baseDn);
            if (debug) log.debug("Partition: "+partition);
            
            if ((partition == null) && !baseDn.isEmpty()) {
            	if (debug) log.debug(baseDn+" is a static entry");
                return next.search(base, env, filter, searchControls);
            }

            if (searchControls != null && searchControls.getReturningAttributes() != null) {

                if (baseDn.isEmpty() && searchControls.getSearchScope() == SearchControls.OBJECT_SCOPE) {

                    NamingEnumeration ne = next.search(base, env, filter, searchControls);
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                    javax.naming.directory.Attributes attrs = sr.getAttributes();

                    SearchRequest request = new SearchRequest();
                    request.setDn(baseDn);
                    request.setFilter("(objectClass=*)");
                    request.setScope(SearchRequest.SCOPE_BASE);
                    request.setDereference(SearchRequest.DEREF_ALWAYS);
                    request.setAttributes(searchControls == null ? null : searchControls.getReturningAttributes());

                    SearchResponse response = new SearchResponse();

                    session.search(request, response);

                    org.safehaus.penrose.session.SearchResult result = (org.safehaus.penrose.session.SearchResult)response.next();
                    org.safehaus.penrose.entry.Entry entry = result.getEntry();

                    org.safehaus.penrose.entry.Attributes attributes = entry.getAttributes();

                    for (NamingEnumeration ne2=attrs.getAll(); ne2.hasMore(); ) {
                        Attribute attr = (Attribute)ne2.next();
                        String name = attr.getID();
                        if (name.equals("vendorName") || name.equals("vendorVersion")) continue;

                        org.safehaus.penrose.entry.Attribute attribute = new org.safehaus.penrose.entry.Attribute(name);
                        for (NamingEnumeration ne3=attr.getAll(); ne3.hasMore(); ) {
                            Object value = ne3.next();
                            attribute.addValue(value);
                        }

                        attributes.add(attribute);
                    }

                    SearchResponse results2 = new SearchResponse();
                    results2.add(entry);
                    results2.close();

                    return new PenroseEnumeration(results2);
                }
            }

            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            Filter newFilter = FilterTool.convert(filter);
            if (debug) {
	            log.debug("Searching \""+base+"\"");
	            log.debug(" - deref: "+deref);
	            log.debug(" - scope: "+scope);
	            log.debug(" - filter: "+newFilter);
	            log.debug(" - attributeNames: "+attributeNames);
            }

            SearchRequest request = new SearchRequest();
            request.setDn(baseDn);
            request.setFilter(newFilter);
            request.setScope(searchControls.getSearchScope());
            request.setSizeLimit(searchControls.getCountLimit());
            request.setTimeLimit(searchControls.getTimeLimit());
            request.setDereference(SearchRequest.DEREF_ALWAYS);
            request.setAttributes(searchControls == null ? null : searchControls.getReturningAttributes());

            SearchResponse response = new SearchResponse();

            session.search(request, response);

            return new PenroseEnumeration(response);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
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
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("modify(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                next.modify(name, modOp, attributes);
                return;
            }

            Collection modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();

                ModificationItem modification = new ModificationItem(modOp, attribute);
                modifications.add(modification);
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            session.modify(dn, modifications);
        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void modify(
            NextInterceptor next,
            LdapDN name,
            ModificationItem[] modificationItems
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = new DN(name.getUpName());
            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("modify(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                next.modify(name, modificationItems);
                return;
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            Collection modifications = new ArrayList(Arrays.asList(modificationItems));
            session.modify(dn, modifications);
        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
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
            DN dn = new DN(name.getUpName());
            RDN newRdn = new RDN(newDn);

            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("modifyDn(\""+dn+"\")");

            Partition partition = partitionManager.getPartition(dn);
            if (partition == null) {
            	if (debug) log.debug(dn+" is a static entry");
                next.modifyRn(name, newDn, deleteOldDn);
                return;
            }

            Session session = getSession();

            if (session.getBindDn() == null && !allowAnonymousAccess) {
                throw ExceptionTool.createNamingException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
            }

            session.modrdn(dn, newRdn, deleteOldDn);
        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
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
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("move(\""+dn+"\")");
        next.move(oriChildName, newParentName, newRn, deleteOldRn);
    }

    public void move(
            NextInterceptor next,
            LdapDN oriChildName,
            LdapDN newParentName
    ) throws NamingException {

        log.debug("===============================================================================");
        String dn = oriChildName.getUpName();
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("move(\""+dn+"\")");
        next.move( oriChildName, newParentName );
    }
}
