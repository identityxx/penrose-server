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
package org.safehaus.penrose.adapter.ldap;

import javax.naming.directory.*;
import javax.naming.*;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsResponseControl;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.connector.ConnectorSearchResult;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.ldap.LDAPClient;
import org.safehaus.penrose.control.Control;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPAdapter extends Adapter {

    public final static String BASE_DN        = "baseDn";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";

    public final static String PAGE_SIZE      = "pageSize";
    public final static int DEFAULT_PAGE_SIZE = 1000;

    private LDAPClient client;

    public void init() throws Exception {
        Map parameters = getParameters();
        client = new LDAPClient(parameters);
    }

    public Object openConnection() throws Exception {
        return new LDAPClient(client, getParameters());
    }

    public RDN getPkValues(SourceConfig sourceConfig, javax.naming.directory.SearchResult sr) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        javax.naming.directory.Attributes attrs = sr.getAttributes();
        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = fieldConfig.getName();
            if (name.equals("objectClass")) continue;

            javax.naming.directory.Attribute attr = attrs.get(fieldConfig.getOriginalName());
            if (attr == null) continue;

            Collection values = new ArrayList();

            for (NamingEnumeration ne = attr.getAll(); ne.hasMore(); ) {
                Object value = ne.next();
                values.add(value);
            }

            rb.set(name, values.iterator().next());
        }

        return rb.toRdn();
    }

    public DN getRecord(
            Partition partition,
            SourceMapping sourceMapping,
            javax.naming.directory.SearchResult sr,
            AttributeValues record
    ) throws Exception {

        DN dn = new DN(sr.getName());

        String sourceName = sourceMapping.getName();
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        RDN rdn = dn.getRdn();
        record.add(sourceName+".primaryKey", rdn);

        javax.naming.directory.Attributes attrs = sr.getAttributes();
        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = sourceName+"."+fieldConfig.getName();
            //if (name.equals("objectClass")) continue;

            javax.naming.directory.Attribute attr = attrs.get(fieldConfig.getOriginalName());
            if (attr == null) {
                if (fieldConfig.isPrimaryKey()) return null;
                continue;
            }

            for (NamingEnumeration ne = attr.getAll(); ne.hasMore(); ) {
                Object value = ne.next();
                record.add(name, value);
            }
        }

        return dn;
    }

    public void modifyAdd(SourceConfig sourceConfig, AttributeValues entry) throws LDAPException {
        DirContext ctx = null;
        try {
            log.debug("Modify Add:");

            DN dn = getDn(sourceConfig, entry);
            log.debug("Replacing attributes "+dn);

            Collection fields = sourceConfig.getFieldConfigs();
            List list = new ArrayList();

            for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                if (name.startsWith("primaryKey.")) continue;

                Set set = (Set)entry.get(name);
                if (set.isEmpty()) continue;

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
                if (fieldConfig == null) continue;

                javax.naming.directory.Attribute attribute = new BasicAttribute(name);
                for (Iterator j = set.iterator(); j.hasNext(); ) {
                    Object v = j.next();
                    if ("unicodePwd".equals(name)) {
                        attribute.add(PasswordUtil.toUnicodePassword(v));
                    } else {
                        attribute.add(v);
                    }
                    log.debug(" - "+name+": "+v);
                }
                list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute));
            }

            log.debug("Updating "+dn);

            ModificationItem mods[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);

            ctx = ((LDAPClient)openConnection()).getContext();
            ctx.modifyAttributes(dn.toString(), mods);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public int modifyDelete(SourceConfig sourceConfig, AttributeValues entry) throws Exception {

        log.debug("Modify Delete:");

        DN dn = getDn(sourceConfig, entry);
        log.debug("Deleting attributes in "+dn);

        List list = new ArrayList();
        Collection fields = sourceConfig.getFieldConfigs();

        for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
            if (fieldConfig == null) continue;

            Set set = (Set)entry.get(name);
            javax.naming.directory.Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                String value = (String)j.next();
                log.debug(" - "+name+": "+value);
                attribute.add(value);
            }
            list.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attribute));
        }

        ModificationItem mods[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);

        DirContext ctx = null;
        try {
            ctx = ((LDAPClient)openConnection()).getContext();
            ctx.modifyAttributes(dn.toString(), mods);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ExceptionUtil.getReturnCode(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }

        return LDAPException.SUCCESS;
    }

    public javax.naming.directory.Attributes convertAttributes(Attributes attributes) throws Exception {
        javax.naming.directory.Attributes ldapAttributes = new BasicAttributes();

        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getName();

            javax.naming.directory.Attribute ldapAttribute = new BasicAttribute(name);
            for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                Object value = j.next();
                ldapAttribute.add(value);
            }

            ldapAttributes.put(ldapAttribute);
        }

        return ldapAttributes;
    }

    public Collection convertModifications(Collection modifications) throws Exception {
        Collection list = new ArrayList();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            javax.naming.directory.Attribute ldapAttribute = new BasicAttribute(attributeName);
            for (Iterator j=attributeValues.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if ("unicodePwd".equals(attributeName)) { // need to encode unicodePwd
                    ldapAttribute.add(PasswordUtil.toUnicodePassword(value));

                } else {
                    ldapAttribute.add(value);
                }
                ldapAttribute.add(value);
            }

            list.add(new ModificationItem(type, ldapAttribute));
        }

        return list;
    }

    public Collection convertControls(Collection controls) throws Exception {
        Collection list = new ArrayList();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            Control control = (Control)i.next();

            String oid = control.getOid();
            boolean critical = control.isCritical();
            byte[] value = control.getValue();

            list.add(new javax.naming.ldap.BasicControl(oid, critical, value));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            SourceConfig sourceConfig,
            RDN pk,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws LDAPException {

        DirContext ctx = null;
        try {
            DN dn = getDn(sourceConfig, pk);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Add "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            javax.naming.directory.Attributes attributes = new BasicAttributes();

            String objectClasses = sourceConfig.getParameter(OBJECT_CLASSES);

            javax.naming.directory.Attribute ocAttribute = new BasicAttribute("objectClass");
            for (StringTokenizer st = new StringTokenizer(objectClasses, ","); st.hasMoreTokens(); ) {
                String objectClass = st.nextToken().trim();
                ocAttribute.add(objectClass);
            }
            attributes.put(ocAttribute);

            log.debug("Adding attributes:");
            for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                if (name.startsWith("primaryKey.")) continue;

                Collection values = sourceValues.get(name);
                if (values.isEmpty()) continue;

                javax.naming.directory.Attribute attribute = new BasicAttribute(name);
                for (Iterator j=values.iterator(); j.hasNext(); ) {
                    Object value = j.next();

                    if ("unicodePwd".equals(name)) {
                        attribute.add(PasswordUtil.toUnicodePassword(value));
                    } else {
                        attribute.add(value);
                    }
                    log.debug(" - "+name+": "+value);
                }
                attributes.put(attribute);
            }

            log.debug("Adding "+dn);
            ctx = ((LDAPClient)openConnection()).getContext();
            ctx.createSubcontext(dn.toString(), attributes);

        } catch (NameAlreadyBoundException e) {
            modifyAdd(sourceConfig, sourceValues);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void add(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+names, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        AddRequestBuilder builder = new AddRequestBuilder(
                this,
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                request,
                response
        );

        DirContext ctx = null;
        try {

            Collection requests = builder.generate();
            for (Iterator i=requests.iterator(); i.hasNext(); ) {
                AddRequest newRequest = (AddRequest)i.next();

                String dn = newRequest.getDn().toString();
                javax.naming.directory.Attributes ldapAttributes = convertAttributes(newRequest.getAttributes());

                log.debug("Adding "+dn);
                ctx = ((LDAPClient)openConnection()).getContext();
                ctx.createSubcontext(dn.toString(), ldapAttributes);
            }

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            SourceConfig sourceConfig,
            RDN pk,
            BindRequest request,
            BindResponse response
    ) throws LDAPException {

        try {
            DN dn = getDn(sourceConfig, pk);
            String password = request.getPassword();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Bind", 80));
                log.debug(Formatter.displayLine(" - Bind DN : "+dn, 80));
                log.debug(Formatter.displayLine(" - Password: "+password, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, getParameter(Context.INITIAL_CONTEXT_FACTORY));
            env.put(Context.PROVIDER_URL, client.getUrl());
            env.put(Context.SECURITY_PRINCIPAL, dn);
            env.put(Context.SECURITY_CREDENTIALS, password);

            DirContext c = new InitialDirContext(env);
            c.close();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);
        }
    }

    public void bind(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        
        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+names, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        BindRequestBuilder builder = new BindRequestBuilder(
                this,
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                request,
                response
        );

        DirContext ctx = null;
        try {

            BindRequest newRequest = builder.generate();

            String dn = newRequest.getDn().toString();
            String password = newRequest.getPassword();

            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, getParameter(Context.INITIAL_CONTEXT_FACTORY));
            env.put(Context.PROVIDER_URL, client.getUrl());
            env.put(Context.SECURITY_PRINCIPAL, dn);
            env.put(Context.SECURITY_CREDENTIALS, password);

            log.debug("Binding as "+dn);
            ctx = new InitialDirContext(env);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            SourceConfig sourceConfig,
            RDN pk,
            DeleteRequest request,
            DeleteResponse response
    ) throws LDAPException {

        DirContext ctx = null;
        try {
            DN dn = getDn(sourceConfig, pk);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Delete "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ctx = ((LDAPClient)openConnection()).getContext();
            ctx.destroySubcontext(dn.toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void delete(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+names, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        DeleteRequestBuilder builder = new DeleteRequestBuilder(
                this,
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                request,
                response
        );

        DirContext ctx = null;
        try {

            Collection requests = builder.generate();
            for (Iterator i=requests.iterator(); i.hasNext(); ) {
                DeleteRequest newRequest = (DeleteRequest)i.next();

                String dn = newRequest.getDn().toString();

                log.debug("Deleting "+dn);
                ctx = ((LDAPClient)openConnection()).getContext();
                ctx.destroySubcontext(dn.toString());
            }

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            SourceConfig sourceConfig,
            RDN pk,
            Collection modifications,
            ModifyRequest request,
            ModifyResponse response
    ) throws LDAPException {

        DirContext ctx = null;
        try {
            DN dn = getDn(sourceConfig, pk);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Modify "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            List list = new ArrayList();

            for (Iterator i=modifications.iterator(); i.hasNext(); ) {
                Modification modification = (Modification)i.next();

                int type = modification.getType();
                Attribute attribute = modification.getAttribute();
                String name = attribute.getName();

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
                if (fieldConfig == null) continue;

                ModificationItem mi = null;
                if ("unicodePwd".equals(name) && type == Modification.ADD) { // need to encode unicodePwd
                    javax.naming.directory.Attribute newAttribute = new BasicAttribute(fieldConfig.getOriginalName());
                    for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                        Object value = j.next();
                        newAttribute.add(PasswordUtil.toUnicodePassword(value));
                    }

                    mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, newAttribute);

                } else {
                    javax.naming.directory.Attribute newAttribute = new BasicAttribute(fieldConfig.getOriginalName());
                    for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                        Object value = j.next();
                        newAttribute.add(value);
                    }
                    mi = new ModificationItem(type, newAttribute);
                }

                list.add(mi);
            }

            ModificationItem mods[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);
            ctx = ((LDAPClient)openConnection()).getContext();
            ctx.modifyAttributes(dn.toString(), mods);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void modify(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+names, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        ModifyRequestBuilder builder = new ModifyRequestBuilder(
                this,
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                request,
                response
        );

        DirContext ctx = null;
        try {

            Collection requests = builder.generate();
            for (Iterator i=requests.iterator(); i.hasNext(); ) {
                ModifyRequest newRequest = (ModifyRequest)i.next();

                String dn = newRequest.getDn().toString();

                Collection list = convertModifications(newRequest.getModifications());
                ModificationItem modifications[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);

                log.debug("Modifying "+dn);
                ctx = ((LDAPClient)openConnection()).getContext();
                ctx.modifyAttributes(dn.toString(), modifications);
            }

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            SourceConfig sourceConfig,
            RDN oldEntry,
            RDN newRdn,
            boolean deleteOldRdn,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws LDAPException {

        DirContext ctx = null;
        try {
            DN dn = getDn(sourceConfig, oldEntry);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("ModRDN "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displayLine(" - New RDN: "+newRdn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ctx = ((LDAPClient)openConnection()).getContext();
            ctx.rename(dn.toString(), newRdn.toString());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void modrdn(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+names, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        ModRdnRequestBuilder builder = new ModRdnRequestBuilder(
                this,
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                request,
                response
        );

        DirContext ctx = null;
        try {
            Collection requests = builder.generate();
            for (Iterator i=requests.iterator(); i.hasNext(); ) {
                ModRdnRequest newRequest = (ModRdnRequest)i.next();

                String dn = newRequest.getDn().toString();
                String newRdn = newRequest.getNewRdn().toString();

                ctx = ((LDAPClient)openConnection()).getContext();
                ctx.rename(dn, newRdn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceConfig sourceConfig,
            SearchRequest request,
            SearchResponse response
    ) throws LDAPException {

        boolean debug = log.isDebugEnabled();
        Filter filter = request.getFilter();

        DNBuilder db = new DNBuilder();
        db.set(sourceConfig.getParameter(BASE_DN));

        String ldapScope = sourceConfig.getParameter(SCOPE);
        String ldapFilter = sourceConfig.getParameter(FILTER);

        String s = sourceConfig.getParameter(PAGE_SIZE);
        int pageSize = s == null ? DEFAULT_PAGE_SIZE : Integer.parseInt(s);

        db.append(client.getSuffix());
        if (filter != null) {
            ldapFilter = "(&"+ldapFilter+filter+")";

            if (filter instanceof SimpleFilter) {
                db.prepend(filter.toString());
                ldapScope = "OBJECT";
            }
        }

        DN ldapBase = db.toDn();
        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - Base DN: "+ldapBase, 80));
            log.debug(Formatter.displayLine(" - Scope: "+ldapScope, 80));
            log.debug(Formatter.displayLine(" - Filter: "+ldapFilter, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SearchControls sc = new SearchControls();
        if ("OBJECT".equals(ldapScope)) {
            sc.setSearchScope(SearchControls.OBJECT_SCOPE);

        } else if ("ONELEVEL".equals(ldapScope)) {
            sc.setSearchScope(SearchControls.ONELEVEL_SCOPE);

        } else if ("SUBTREE".equals(ldapScope)) {
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
        }
        sc.setCountLimit(request.getSizeLimit());
        sc.setTimeLimit((int) request.getTimeLimit());

        LdapContext ctx = null;
        try {
            ctx = ((LDAPClient)openConnection()).getContext();

            javax.naming.ldap.Control[] controls = new javax.naming.ldap.Control[] { new PagedResultsControl(pageSize, javax.naming.ldap.Control.NONCRITICAL) };
            ctx.setRequestControls(controls);

            int page = 0;
            byte[] cookie = null;

            do {
                if (debug) log.debug("Searching page #"+page);
                NamingEnumeration ne = ctx.search(ldapBase.toString(), ldapFilter, sc);

                if (debug) log.debug("Results from page #"+page+":");
                while (ne.hasMore()) {
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();

                    AttributeValues record = new AttributeValues();
                    DN dn = getRecord(partition, sourceMapping, sr, record);

                    if (debug) {
                        LDAPFormatter.printRecord(dn, record);
                    }

                    ConnectorSearchResult result = new ConnectorSearchResult(record);
                    result.setEntryMapping(entryMapping);
                    //result.setSourceMapping(sourceMapping);
                    //result.setSourceConfig(sourceConfig);

                    response.add(result);
                }

                // get cookie returned by server
                controls = ctx.getResponseControls();
                if (controls != null) {
                    for (int i = 0; i < controls.length; i++) {
                        if (controls[i] instanceof PagedResultsResponseControl) {
                            PagedResultsResponseControl prrc = (PagedResultsResponseControl)controls[i];
                            cookie = prrc.getCookie();
                        }
                    }
                }

                // pass cookie back to server for the next page
                controls = new javax.naming.ldap.Control[] { new PagedResultsControl(pageSize, cookie, javax.naming.ldap.Control.CRITICAL) };
                ctx.setRequestControls(controls);

                page++;

            } while (cookie != null && cookie.length != 0);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+names, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        SearchRequestBuilder builder = new SearchRequestBuilder(
                this,
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                request,
                response
        );

        LdapContext ctx = null;
        try {
            SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            SearchRequest newRequest = builder.generate();

            DNBuilder db = new DNBuilder();
            db.set(newRequest.getDn());
            db.append(client.getSuffix());

            String baseDn = db.toString();
            String filter = newRequest.getFilter() == null ? "(objectClass=*)" : newRequest.getFilter().toString();

            SearchControls sc = new SearchControls();
            sc.setSearchScope(newRequest.getScope());
            sc.setCountLimit(newRequest.getSizeLimit());
            sc.setTimeLimit((int)newRequest.getTimeLimit());

            ctx = ((LDAPClient)openConnection()).getContext();

            String s = sourceConfig.getParameter(LDAPAdapter.PAGE_SIZE);
            int pageSize = s == null ? LDAPAdapter.DEFAULT_PAGE_SIZE : Integer.parseInt(s);

            Collection list = convertControls(newRequest.getControls());
            list.add(new PagedResultsControl(pageSize, javax.naming.ldap.Control.NONCRITICAL));

            javax.naming.ldap.Control controls[] = (javax.naming.ldap.Control[])list.toArray(new javax.naming.ldap.Control[list.size()]);
            ctx.setRequestControls(controls);

            int page = 0;
            byte[] cookie = null;

            do {
                if (debug) log.debug("Searching page #"+page+": "+baseDn+" "+filter+" "+LDAPUtil.getScope(newRequest.getScope()));
                NamingEnumeration ne = ctx.search(baseDn, filter, sc);

                if (debug) log.debug("Results from page #"+page+":");
                while (ne.hasMore()) {
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();

                    AttributeValues record = new AttributeValues();
                    DN dn = getRecord(partition, sourceMapping, sr, record);
                    if (dn == null) continue;
                    
                    if (debug) {
                        LDAPFormatter.printRecord(dn, record);
                    }

                    ConnectorSearchResult result = new ConnectorSearchResult(record);
                    result.setEntryMapping(entryMapping);
                    //result.setSourceMapping(sourceMapping);
                    //result.setSourceConfig(sourceConfig);

                    response.add(result);
                }

                // get cookie returned by server
                controls = ctx.getResponseControls();
                if (controls != null) {
                    for (int i = 0; i < controls.length; i++) {
                        if (controls[i] instanceof PagedResultsResponseControl) {
                            PagedResultsResponseControl prrc = (PagedResultsResponseControl)controls[i];
                            cookie = prrc.getCookie();
                        }
                    }
                }

                // pass cookie back to server for the next page
                controls = new javax.naming.ldap.Control[] { new PagedResultsControl(pageSize, cookie, javax.naming.ldap.Control.CRITICAL) };
                ctx.setRequestControls(controls);

                page++;

            } while (cookie != null && cookie.length != 0);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public DN getDn(SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {
        //log.debug("Computing DN for "+source.getName()+" with "+sourceValues);

        RDN pk = sourceConfig.getPrimaryKeyValues(sourceValues);
        //RDN pk = sourceValues.getRdn();

        String baseDn = sourceConfig.getParameter(BASE_DN);
        //log.debug("Base DN: "+baseDn);

        DNBuilder db = new DNBuilder();
        db.append(pk);
        db.append(baseDn);
        db.append(client.getSuffix());

        //log.debug("DN: "+sb);

        return db.toDn();
    }

    public DN getDn(SourceConfig sourceConfig, RDN rdn) throws Exception {
        String baseDn = sourceConfig.getParameter(BASE_DN);
        //log.debug("Base DN: "+baseDn);

        DNBuilder db = new DNBuilder();
        db.append(rdn);
        db.append(baseDn);
        db.append(client.getSuffix());
        DN dn = db.toDn();

        //log.debug("DN: "+dn);

        return dn;
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws LDAPException {
        return 0;
    }

    public SearchResponse getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws LDAPException {

        SearchResponse response = new SearchResponse();

        //int sizeLimit = 100;

        String ldapBase = "cn=changelog";
        String ldapFilter = "(&(changeNumber>="+lastChangeNumber+")(!(changeNumber="+lastChangeNumber+")))";

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - Base DN: "+ldapBase, 80));
            log.debug(Formatter.displayLine(" - Filter: "+ldapFilter, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

        DirContext ctx = null;
        try {
            ctx = ((LDAPClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

            log.debug("Result:");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                log.debug(" - "+sr.getName()+","+ldapBase);

                RDN rdn = getPkValues(sourceConfig, sr);
                response.add(rdn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            try { response.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (ctx != null) try { ctx.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }

        return response;
    }

    public Filter convert(EntryMapping entryMapping, SubstringFilter filter) throws Exception {

        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        String variable = attributeMapping.getVariable();

        if (variable == null) return null;

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);

        return new SubstringFilter(fieldName, substrings);
    }

    public LDAPClient getClient() throws Exception {
        return new LDAPClient(client, getParameters());
    }
}
