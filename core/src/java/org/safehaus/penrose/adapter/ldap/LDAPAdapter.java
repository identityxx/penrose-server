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
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.ldap.LDAPClient;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;

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

    public Entry createEntry(
            Partition partition,
            SourceRef sourceRef,
            javax.naming.directory.SearchResult sr
    ) throws Exception {

        String sourceName = sourceRef.getAlias();

        DN dn = new DN(sr.getName());
        Entry entry = new Entry(dn);

        Attributes sourceValues = new Attributes();
        entry.setSourceValues(sourceName, sourceValues);

        RDN rdn = dn.getRdn();
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);
            sourceValues.addValue("primaryKey."+name, value);
        }

        javax.naming.directory.Attributes attrs = sr.getAttributes();
        for (Iterator i= sourceRef.getFieldRefs().iterator(); i.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)i.next();

            javax.naming.directory.Attribute attr = attrs.get(fieldRef.getOriginalName());
            if (attr == null) {
                if (fieldRef.isPrimaryKey()) return null;
                continue;
            }

            String fieldName = fieldRef.getName();
            //if (fieldName.equals("objectClass")) continue;

            for (NamingEnumeration ne = attr.getAll(); ne.hasMore(); ) {
                Object value = ne.next();
                sourceValues.addValue(fieldName, value);
            }
        }

        return entry;
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
                if ("unicodePwd".equals(name)) {
                    ldapAttribute.add(PasswordUtil.toUnicodePassword(value));
                } else {
                    ldapAttribute.add(value);
                }
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
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Add "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        AddRequestBuilder builder = new AddRequestBuilder(
                client.getSuffix(),
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
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
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        
        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Bind "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        BindRequestBuilder builder = new BindRequestBuilder(
                client.getSuffix(),
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
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
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        DeleteRequestBuilder builder = new DeleteRequestBuilder(
                client.getSuffix(),
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
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
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i= sourceRefs.iterator(); i.hasNext(); ) {
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
                client.getSuffix(),
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
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
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i= sourceRefs.iterator(); i.hasNext(); ) {
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
                client.getSuffix(),
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
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
            EntryMapping entryMapping,
            Collection sourceRefs,
            AttributeValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+ sourceRefs, 80));
            log.debug(Formatter.displaySeparator(80));

            log.debug("Source values:");
            sourceValues.print();
        }

        SearchRequestBuilder builder = new SearchRequestBuilder(
                partition,
                entryMapping,
                sourceRefs,
                sourceValues,
                penroseContext.getInterpreterManager().newInstance(),
                request,
                response
        );

        LdapContext ctx = null;
        try {
            SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();

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

            Source source = sourceRef.getSource();
            String s = source.getParameter(LDAPAdapter.PAGE_SIZE);
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

                    Entry entry = createEntry(partition, sourceRef, sr);
                    if (entry == null) continue;
                    
                    if (debug) {
                        LDAPFormatter.printEntry(entry);
                    }

                    response.add(entry);
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

        RDN pk = sourceConfig.getPrimaryKeyValues(sourceValues);

        String baseDn = sourceConfig.getParameter(BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(pk);
        db.append(baseDn);
        db.append(client.getSuffix());

        return db.toDn();
    }

    public DN getDn(SourceConfig sourceConfig, RDN rdn) throws Exception {

        String baseDn = sourceConfig.getParameter(BASE_DN);

        DNBuilder db = new DNBuilder();
        db.append(rdn);
        db.append(baseDn);
        db.append(client.getSuffix());
        DN dn = db.toDn();

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
