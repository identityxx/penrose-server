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
import org.apache.directory.shared.ldap.message.ModificationItemImpl;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import java.util.*;

import com.identyx.javabackend.*;
import com.identyx.javabackend.SearchResult;

/**
 * @author Endi S. Dewata
 */
public class PenroseInterceptor extends BaseInterceptor {

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

    public Session getSession() throws Exception {

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        Session session = backend.getSession(bindDn);

        if (session == null) {
            session = backend.createSession(bindDn);
            if (session == null) throw new ServiceUnavailableException();
        }

        return session;
    }

    public void removeSession() throws Exception {

        String bindDn = getPrincipal() == null ? null : getPrincipal().getJndiName().getUpName();

        backend.closeSession(bindDn);
    }

    public void bind(
            NextInterceptor next,
            LdapDN bindDn,
            byte[] credentials,
            List mechanisms,
            String saslAuthId) throws NamingException {

        log.debug("===============================================================================");
        try {
            log.debug("bind(\""+bindDn+"\")");
            //log.debug(" - mechanisms: "+mechanisms);
            //log.debug(" - sslAuthId: "+saslAuthId);

            String password = new String(credentials);
            //log.debug(" - password: "+password);

            next.bind(bindDn, credentials, mechanisms, saslAuthId);

            Session session = getSession();

            DN dn = backend.createDn(bindDn.getUpName());

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
        try {
            log.debug("unbind(\""+bindDn+"\")");

            Session session = getSession();

            session.unbind();

            nextInterceptor.unbind(bindDn);

        } catch (LDAPException e) {
            throw ExceptionTool.createNamingException(e);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
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

            Session session = getSession();

            com.identyx.javabackend.Attributes attributes = backend.createAttributes();
            for (NamingEnumeration ne = attrs.getAll(); ne.hasMore(); ) {
                javax.naming.directory.Attribute attr = (javax.naming.directory.Attribute)ne.next();
                String attributeName = attr.getID();

                com.identyx.javabackend.Attribute attribute = backend.createAttribute(attributeName);
                for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attribute.addValue(value);
                }

                attributes.add(attribute);
            }

            session.add(dn, attributes);

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

            Session session = getSession();

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

            Session session = getSession();

            session.delete(dn);

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

    public Attributes getRootDSE(NextInterceptor next) throws NamingException {
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

            Session session = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = session.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_ONE
            );

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

            Session session = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = session.search(
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

    public Attributes lookup(
            NextInterceptor next,
            LdapDN name,
            String[] attrIds
    ) throws NamingException {

        log.debug("===============================================================================");
        try {
            DN dn = backend.createDn(name.getUpName());
            log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+")");

            boolean debug = log.isDebugEnabled();
            if (debug) log.debug("lookup(\""+dn+"\", "+Arrays.asList(attrIds)+")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            Session session = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = session.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_BASE
            );

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

        boolean debug = log.isDebugEnabled();

        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(name.getUpName());
            log.debug("lookup(\""+dn+"\")");

            if (!backend.contains(dn)) {
            	if (debug) log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            Session session = getSession();

            Filter filter = backend.createFilter("(objectClass=*)");

            SearchResponse response = session.search(
                    dn,
                    filter,
                    SearchRequest.SCOPE_BASE
            );

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

        boolean debug = log.isDebugEnabled();

        log.debug("===============================================================================");

        try {
            DN dn = backend.createDn(base.getUpName());
            if (debug) {
                StringBuffer sb = new StringBuffer();
                filter.printToBuffer(sb);
            	log.debug("search(\""+base+"\")");
            }

            Session session = getSession();

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

            Collection modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attr = (Attribute)e.nextElement();

                com.identyx.javabackend.Attribute attribute = backend.createAttribute(attr.getID());
                for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attribute.addValue(value);
                }

                Modification modification = backend.createModification(modOp, attribute);
                modifications.add(modification);
            }

            Session session = getSession();

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

            Collection modifications = new ArrayList();

            for (int i=0; i<modificationItems.length; i++) {
                ModificationItem mi = modificationItems[i];
                int modOp = mi.getModificationOp();
                Attribute attr = mi.getAttribute();

                com.identyx.javabackend.Attribute attribute = backend.createAttribute(attr.getID());
                for (NamingEnumeration ne2 = attr.getAll(); ne2.hasMore(); ) {
                    Object value = ne2.next();
                    attribute.addValue(value);
                }

                Modification modification = backend.createModification(modOp, attribute);
                modifications.add(modification);
            }

            Session session = getSession();

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

            Session session = getSession();

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
