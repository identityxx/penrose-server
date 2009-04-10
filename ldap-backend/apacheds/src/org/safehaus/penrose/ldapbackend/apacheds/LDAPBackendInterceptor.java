/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.ldapbackend.apacheds;

import org.apache.directory.server.core.interceptor.BaseInterceptor;
import org.apache.directory.server.core.interceptor.NextInterceptor;
import org.apache.directory.server.core.DirectoryServiceConfiguration;
import org.apache.directory.server.core.configuration.InterceptorConfiguration;
import org.apache.directory.shared.ldap.name.LdapDN;
import org.apache.directory.shared.ldap.filter.ExprNode;
import org.apache.directory.shared.ldap.message.ModificationItemImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.ldapbackend.*;

import javax.naming.NamingException;
import javax.naming.ServiceUnavailableException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPBackendInterceptor extends BaseInterceptor {

    public Logger log = LoggerFactory.getLogger(getClass());

    Backend backend;

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public void init(
            DirectoryServiceConfiguration factoryCfg,
            InterceptorConfiguration cfg
    ) throws NamingException {

        super.init(factoryCfg, cfg);
    }

    public Connection getSession() throws Exception {

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        Connection connection = backend.getConnection(bindDn);

        if (connection == null) {
            ConnectRequest request = backend.createConnectRequest();
            request.setConnectionId(bindDn);

            connection = backend.connect(request);
            if (connection == null) throw new ServiceUnavailableException();
        }

        return connection;
    }

    public void removeSession() throws Exception {

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        DisconnectRequest request = backend.createDisconnectRequest();
        request.setConnectionId(bindDn);

        backend.disconnect(request);
    }

    public void bind(
            NextInterceptor next,
            LdapDN bindDn,
            byte[] password,
            List mechanisms,
            String saslAuthId) throws NamingException {

        log.debug("===============================================================================");
        try {
            log.debug("bind(\""+bindDn+"\")");
            //log.debug(" - mechanisms: "+mechanisms);
            //log.debug(" - sslAuthId: "+saslAuthId);
            //log.debug(" - password: "+password);

            next.bind(bindDn, password, mechanisms, saslAuthId);

            Connection connection = getSession();

            DN dn = backend.createDn(bindDn.getUpName());

            connection.bind(dn, password);

            //log.debug("Bind successful.");

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void unbind(
            NextInterceptor nextInterceptor,
            LdapDN bindDn
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            log.debug("unbind(\""+bindDn+"\")");

            Connection connection = getSession();

            connection.unbind();

            nextInterceptor.unbind(bindDn);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public void add(
            NextInterceptor next,
            LdapDN name,
            Attributes attrs
    ) throws NamingException {

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("add(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                next.add(name, attrs);
                return;
            }

            Connection connection = getSession();

            org.safehaus.penrose.ldapbackend.Attributes attributes = backend.createAttributes();
            for (NamingEnumeration ne = attrs.getAll(); ne.hasMore(); ) {
                javax.naming.directory.Attribute attr = (javax.naming.directory.Attribute)ne.next();
                String attributeName = attr.getID();

                org.safehaus.penrose.ldapbackend.Attribute attribute = backend.createAttribute(attributeName);
                for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attribute.addValue(value);
                }

                attributes.add(attribute);
            }

            connection.add(dn, attributes);

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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("compare(\""+dn+"\", \""+attributeName+"\", "+value+")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.compare(name, attributeName, value);
            }

            Connection connection = getSession();

            return connection.compare(dn, attributeName, value);

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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("delete(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                next.delete(name);
                return;
            }

            Connection connection = getSession();

            connection.delete(dn);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public LdapDN getMatchedName(NextInterceptor next, LdapDN dn) throws NamingException {

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");
        if (debug) log.debug("getMatchedName(\""+dn+"\")");
        return next.getMatchedName(dn);
    }

    public javax.naming.directory.Attributes getRootDSE(NextInterceptor next) throws NamingException {
        log.debug("===============================================================================");
        log.debug("getRootDSE()");
        return next.getRootDSE();
    }

    public LdapDN getSuffix(NextInterceptor next, LdapDN dn) throws NamingException {
        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");
        if (debug) log.debug("getSuffix(\""+dn+"\")");
        return next.getSuffix(dn);
    }

    public boolean isSuffix(NextInterceptor next, LdapDN name) throws NamingException {
        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");
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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("list(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.list(name);
            }

            Connection connection = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = connection.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_ONE
            );

            return new LDAPBackendEnumeration(response);

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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("hasEntry(\""+dn+"\")");

            if (!backend.contains(dn)) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            if (debug) log.debug("searching \""+dn+"\"");

            Connection connection = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = connection.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_BASE
            );

            return response.hasNext();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public javax.naming.directory.Attributes lookup(
            NextInterceptor next,
            LdapDN name,
            String[] attrIds
    ) throws NamingException {

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");
        try {
            DN dn = backend.createDn(name.getUpName());
            log.debug("lookup(\""+dn+"\", "+ Arrays.asList(attrIds)+")");

            if (debug) log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            Connection connection = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = connection.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_BASE
            );

            org.safehaus.penrose.ldapbackend.SearchResult result = (org.safehaus.penrose.ldapbackend.SearchResult)response.next();

            javax.naming.directory.SearchResult sr = EntryTool.createSearchResult(result);
            return sr.getAttributes();

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionTool.createNamingException(e);
        }
    }

    public javax.naming.directory.Attributes lookup(
            NextInterceptor next,
            LdapDN name
    ) throws NamingException {

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            log.debug("lookup(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            Connection connection = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = connection.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_BASE
            );

            org.safehaus.penrose.ldapbackend.SearchResult result = (org.safehaus.penrose.ldapbackend.SearchResult)response.next();

            javax.naming.directory.SearchResult sr = EntryTool.createSearchResult(result);
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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(base.getUpName());
            if (debug) {
                StringBuffer sb = new StringBuffer();
                filter.printToBuffer(sb);
            	log.debug("search(\""+base+"\")");
            }

            Connection connection = getSession();

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.search(base, env, filter, searchControls);
            }

            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            Filter newFilter = backend.createFilter(FilterTool.convert(filter));
            if (debug) {
	            log.debug("Searching \""+base+"\"");
	            log.debug(" - deref: "+deref);
	            log.debug(" - scope: "+scope);
	            log.debug(" - filter: "+newFilter);
	            log.debug(" - attributeNames: "+attributeNames);
            }

            SearchRequest request = backend.createSearchRequest();
            request.setDn(dn);
            request.setFilter(newFilter);
            request.setScope(searchControls.getSearchScope());
            request.setSizeLimit(searchControls.getCountLimit());
            request.setTimeLimit(searchControls.getTimeLimit());

            if (searchControls != null && searchControls.getReturningAttributes() != null) {
                request.setAttributes(Arrays.asList(searchControls.getReturningAttributes()));
            }

            SearchResponse response = backend.createSearchResponse();

            connection.search(request, response);

            return new LDAPBackendEnumeration(response);

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
            javax.naming.directory.Attributes attributes
    ) throws NamingException {

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("modify(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                next.modify(name, modOp, attributes);
                return;
            }

            Collection<Modification> modifications = new ArrayList<Modification>();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                javax.naming.directory.Attribute attr = (javax.naming.directory.Attribute)e.nextElement();

                org.safehaus.penrose.ldapbackend.Attribute attribute = backend.createAttribute(attr.getID());
                for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attribute.addValue(value);
                }

                int type;
                switch (modOp) {
                    case DirContext.ADD_ATTRIBUTE:
                        type = Modification.ADD;
                        break;
                    case DirContext.REMOVE_ATTRIBUTE:
                        type = Modification.DELETE;
                        break;
                    default:
                        type = Modification.REPLACE;
                        break;
                }

                Modification modification = backend.createModification(type, attribute);
                modifications.add(modification);
            }

            Connection connection = getSession();

            connection.modify(dn, modifications);

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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            if (debug) log.debug("modify(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                next.modify(name, (ModificationItemImpl[])modificationItems);
                return;
            }

            Collection<Modification> modifications = new ArrayList<Modification>();

            for (ModificationItem mi : modificationItems) {
                int modOp = mi.getModificationOp();
                Attribute attr = mi.getAttribute();

                org.safehaus.penrose.ldapbackend.Attribute attribute = backend.createAttribute(attr.getID());
                for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore();) {
                    Object value = ne2.next();
                    attribute.addValue(value);
                }

                Modification modification = backend.createModification(modOp, attribute);
                modifications.add(modification);
            }

            Connection connection = getSession();

            connection.modify(dn, modifications);

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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            RDN newRdn = backend.createRdn(newDn);

            if (debug) log.debug("modifyDn(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                next.modifyRn(name, newDn, deleteOldDn);
                return;
            }

            Connection connection = getSession();

            connection.modrdn(dn, newRdn, deleteOldDn);

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

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");
        String dn = oriChildName.getUpName();
        if (debug) log.debug("move(\""+dn+"\")");
        next.move(oriChildName, newParentName, newRn, deleteOldRn);
    }

    public void move(
            NextInterceptor next,
            LdapDN oriChildName,
            LdapDN newParentName
    ) throws NamingException {

        boolean debug = log.isDebugEnabled();
        log.debug("===============================================================================");
        String dn = oriChildName.getUpName();
        if (debug) log.debug("move(\""+dn+"\")");
        next.move(oriChildName, newParentName );
    }
}
