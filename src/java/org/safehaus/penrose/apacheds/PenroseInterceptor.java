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
package org.safehaus.penrose.apacheds;

import org.apache.ldap.server.interceptor.BaseInterceptor;
import org.apache.ldap.server.interceptor.NextInterceptor;
import org.apache.ldap.server.configuration.InterceptorConfiguration;
import org.apache.ldap.server.DirectoryServiceConfiguration;
import org.apache.ldap.common.filter.ExprNode;
import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.config.Config;
import org.ietf.ldap.*;

import javax.naming.*;
import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseInterceptor extends BaseInterceptor {

    public Logger log = Logger.getLogger(getClass());

    Penrose penrose;
    ApacheDSEngineCache entryCache;

    DirectoryServiceConfiguration factoryCfg;

    public void setPenrose(Penrose penrose) throws Exception {
        this.penrose = penrose;
/*
        Cache cache = penrose.getCache();

        entryCache = new ApacheDSEngineCache();
        entryCache.init(cache);
        cache.setEntryDataCache(entryCache);
*/
    }

    public void init(DirectoryServiceConfiguration factoryCfg, InterceptorConfiguration cfg) throws NamingException
    {
        super.init(factoryCfg, cfg);
        this.factoryCfg = factoryCfg;
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

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                next.add(upName, normName, attributes);
                return;
            }
/*
            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.add(upName, normName, attributes);
                return;
            }

            if (!ed.isDynamic()) {
                log.debug(dn+" is a static entry");
                next.add(upName, normName, attributes);
                return;
            }
*/
            //log.debug("suffix: "+getSuffix(next, normName, true));
            //if (getSuffix(next, normName, true).equals(normName)) return;

            LDAPAttributeSet attributeSet = new LDAPAttributeSet();
            for (Enumeration e = attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();
                LDAPAttribute attr = new LDAPAttribute(attribute.getID());

                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    attr.addValue(value.toString());
                }

                attributeSet.add(attr);
            }

            LDAPEntry ldapEntry = new LDAPEntry(upName, attributeSet);

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            int rc = connection.add(ldapEntry);

            connection.close();

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
            log.debug("compare(\""+dn+"\")");

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                return next.compare(name, attributeName, value);
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.compare(name, attributeName, value);
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            int rc = connection.compare(dn, attributeName, value.toString());

            connection.close();

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

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                next.delete(name);
                return;
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.delete(name);
                return;
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            int rc = connection.delete(dn);

            connection.close();

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


            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                return next.list(name);
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.list(name);
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            String baseDn = dn.toString();
            SearchResults results = connection.search(
                    baseDn,
                    LDAPConnection.SCOPE_ONE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)",
                    new ArrayList());
/*
            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }
*/
            return new ApacheDSEnumeration(results);

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

            Config config = penrose.getConfig(dn);
            if (config == null) {
                //log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            log.debug("===============================================================================");
            log.debug("hasEntry(\""+dn+"\") as "+principalDn);

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.hasEntry(name);
            }

            log.debug("searching \""+dn+"\"");

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            String base = name.toString();
            SearchResults results = connection.search(
                    base,
                    LDAPConnection.SCOPE_BASE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)", new ArrayList());

            boolean result = results.getReturnCode() == LDAPException.SUCCESS && results.size() == 1;

            connection.close();

            return result;

        } catch (NamingException e) {
            e.printStackTrace();
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

            log.debug("===============================================================================");
            log.debug("lookup(\""+dn+"\", "+attrIds+")");

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.lookup(name, attrIds);
            }

            for (int i=0; attrIds != null && i<attrIds.length; i++) {
                log.debug("- "+attrIds[i]);
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            String baseDn = dn.toString();
            SearchResults results = connection.search(
                    baseDn,
                    LDAPConnection.SCOPE_BASE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)",
                    new ArrayList());

            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }

            LDAPEntry result = (LDAPEntry)results.next();

            LDAPAttributeSet attributeSet = result.getAttributeSet();
            Attributes attributes = new BasicAttributes();

            for (Iterator j = attributeSet.iterator(); j.hasNext(); ) {
                LDAPAttribute attribute = (LDAPAttribute)j.next();
                Attribute attr = new BasicAttribute(attribute.getName());

                for (Enumeration k=attribute.getStringValues(); k.hasMoreElements(); ) {
                    String value = (String)k.nextElement();
                    attr.add(value);
                }

                attributes.put(attr);
            }

            return attributes;

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

            log.debug("===============================================================================");
            log.debug("lookup(\""+dn+"\")");

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                return next.lookup(name);
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            String baseDn = dn.toString();
            SearchResults results = connection.search(
                    baseDn,
                    LDAPConnection.SCOPE_BASE,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    "(objectClass=*)",
                    new ArrayList());

            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }

            LDAPEntry result = (LDAPEntry)results.next();

            LDAPAttributeSet attributeSet = result.getAttributeSet();
            Attributes attributes = new BasicAttributes();

            for (Iterator j = attributeSet.iterator(); j.hasNext(); ) {
                LDAPAttribute attribute = (LDAPAttribute)j.next();
                Attribute attr = new BasicAttribute(attribute.getName());

                for (Enumeration k=attribute.getStringValues(); k.hasMoreElements(); ) {
                    String value = (String)k.nextElement();
                    attr.add(value);
                }

                attributes.put(attr);
            }

            return attributes;

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

        //entryCache.setNextInterceptor(next);
        //entryCache.setContext(getContext());

        try {
            String baseDn = base.toString();

            log.debug("===============================================================================");
            log.debug("search(\""+baseDn+"\") as "+principalDn);

            if (searchControls != null && searchControls.getReturningAttributes() != null) {
                Collection requestedAttrs = Arrays.asList(searchControls.getReturningAttributes());

                if ("".equals(baseDn) && searchControls.getSearchScope() == SearchControls.OBJECT_SCOPE) {

                    NamingEnumeration ne = next.search(base, env, filter, searchControls);
                    SearchResult sr = (SearchResult)ne.next();
                    Attributes attributes = sr.getAttributes();

                    if (requestedAttrs.contains("*") || requestedAttrs.contains("vendorName")) {
                        Attribute attr = attributes.get("vendorName");
                        if (attr == null) {
                            attr = new BasicAttribute("vendorName");
                            attributes.put(attr);
                        } else {
                            attr.clear();
                        }
                        attr.add("Identyx Corporation");
                    }

                    if (requestedAttrs.contains("*") || requestedAttrs.contains("vendorVersion")) {
                        Attribute attr = attributes.get("vendorVersion");
                        if (attr == null) {
                            attr = new BasicAttribute("vendorVersion");
                            attributes.put(attr);
                        } else {
                            attr.clear();
                        }
                        attr.add("Penrose Virtual Directory Server 0.9.8");
                    }

                    if (requestedAttrs.contains("*") || requestedAttrs.contains("namingContexts")) {
                        Attribute attr = attributes.get("namingContexts");
                        if (attr == null) {
                            attr = new BasicAttribute("namingContexts");
                            attributes.put(attr);
                        }
                        Collection configs = penrose.getConfigs();
                        for (Iterator i=configs.iterator(); i.hasNext(); ) {
                            Config config = (Config)i.next();
                            Collection roots = config.getRootEntryDefinitions();
                            for (Iterator j=roots.iterator(); j.hasNext(); ) {
                                EntryDefinition ed = (EntryDefinition)j.next();
                                if (attr.contains(ed.getDn())) continue;
                                attr.add(ed.getDn());
                            }
                        }
                    }

                    List list = new ArrayList();
                    list.add(sr);

                    return new PenroseEnumeration(list.iterator());
                }
            }

            Config config = penrose.getConfig(baseDn);
            if (config == null) {
                log.debug(baseDn+" is a static entry");
                return next.search(base, env, filter, searchControls);
            }
/*
            EntryDefinition ed = config.findEntryDefinition(baseDn);
            if (ed == null) {
                log.debug(baseDn+" is a static entry");
                return next.search(base, env, filter, searchControls);
            }

            if (!ed.isDynamic()) {
                log.debug(baseDn+" is a static entry");
                NamingEnumeration ne = next.search(base, env, filter, searchControls);

                while (ne.hasMore()) {
                    SearchResult sr = (SearchResult)ne.next();
                    log.debug("dn: "+sr.getName());

                    Attributes attributes = sr.getAttributes();
                    for (NamingEnumeration ne2 = attributes.getAll(); ne2.hasMore(); ) {
                        Attribute attribute = (Attribute)ne2.next();
                        String name = attribute.getID();
                        log.debug(name+": "+attribute.get());
                    }
                }

                return ne;
            }
*/
            String deref = (String)env.get("java.naming.ldap.derefAliases");
            int scope = searchControls.getSearchScope();
            String returningAttributes[] = searchControls.getReturningAttributes();
            List attributeNames = returningAttributes == null ? new ArrayList() : Arrays.asList(returningAttributes);

            StringBuffer sb = new StringBuffer();
            filter.printToBuffer(sb);
            String newFilter = sb.toString();

            log.debug("Searching \""+base+"\"");
            log.debug(" - deref: "+deref);
            log.debug(" - scope: "+scope);
            log.debug(" - filter: "+newFilter+" ("+filter.getClass().getName()+")");
            log.debug(" - attributeNames: "+attributeNames);

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            SearchResults results = connection.search(
                    baseDn,
                    scope,
                    LDAPSearchConstraints.DEREF_ALWAYS,
                    newFilter,
                    attributeNames);
/*
            int rc = results.getReturnCode();
            connection.close();

            if (rc != LDAPException.SUCCESS) {
                ExceptionUtil.throwNamingException(rc);
            }
*/

            return new ApacheDSEnumeration(results);

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

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modOp, attributes);
                return;
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modOp, attributes);
                return;
            }

            List modifications = new ArrayList();

            for (Enumeration e=attributes.getAll(); e.hasMoreElements(); ) {
                Attribute attribute = (Attribute)e.nextElement();
                String attrName = attribute.getID();

                int op = LDAPModification.REPLACE;
                LDAPAttribute attr = new LDAPAttribute(attrName);

                log.debug("replace: "+attrName);
                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    log.debug(attrName+": "+value);
                    attr.addValue(value.toString());
                }
                log.debug("-");

                LDAPModification modification = new LDAPModification(op, attr);
                modifications.add(modification);
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            int rc = connection.modify(dn.toString(), modifications);

            connection.close();

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

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modificationItems);
                return;
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.modify(name, modificationItems);
                return;
            }

            List modifications = new ArrayList();

            for (int i=0; i<modificationItems.length; i++) {
                ModificationItem mi = modificationItems[i];
                Attribute attribute = mi.getAttribute();
                String attrName = attribute.getID();

                int op = LDAPModification.REPLACE;
                switch (mi.getModificationOp()) {
                    case DirContext.ADD_ATTRIBUTE:
                        log.debug("add: "+attrName);
                        op = LDAPModification.ADD;
                        break;
                    case DirContext.REPLACE_ATTRIBUTE:
                        log.debug("replace: "+attrName);
                        op = LDAPModification.REPLACE;
                        break;
                    case DirContext.REMOVE_ATTRIBUTE:
                        log.debug("delete: "+attrName);
                        op = LDAPModification.DELETE;
                        break;
                }

                LDAPAttribute attr = new LDAPAttribute(attrName);

                for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                    Object value = values.nextElement();
                    log.debug(attrName+": "+value);
                    attr.addValue(value.toString());
                }
                log.debug("-");

                LDAPModification modification = new LDAPModification(op, attr);
                modifications.add(modification);
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            int rc = connection.modify(dn.toString(), modifications);

            connection.close();

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
            String newRn,
            boolean deleteOldRn
            ) throws NamingException {

        Name principalDn = getPrincipal() == null ? null : getPrincipal().getJndiName();

        try {
            String dn = name.toString();

            log.debug("===============================================================================");
            log.debug("modifyRn(\""+dn+"\")");

            Config config = penrose.getConfig(dn);
            if (config == null) {
                log.debug(dn+" is a static entry");
                next.modifyRn(name, newRn, deleteOldRn);
                return;
            }

            EntryDefinition ed = config.findEntryDefinition(dn);
            if (ed == null) {
                log.debug(dn+" is a static entry");
                next.modifyRn(name, newRn, deleteOldRn);
                return;
            }

            PenroseConnection connection = penrose.openConnection();
            connection.setBindDn(principalDn.toString());

            int rc = connection.modrdn(dn.toString(), newRn);

            connection.close();

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
