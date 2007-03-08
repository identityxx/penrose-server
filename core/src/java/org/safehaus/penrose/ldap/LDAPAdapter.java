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
package org.safehaus.penrose.ldap;

import javax.naming.directory.*;
import javax.naming.*;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.connector.ConnectorSearchResult;
import org.safehaus.penrose.entry.*;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPAdapter extends Adapter {

    public final static String BASE_DN        = "baseDn";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";

    private LDAPClient client;

    public void init() throws Exception {
        Map parameters = getParameters();
        client = new LDAPClient(parameters);
    }

    public Object openConnection() throws Exception {
        return new LDAPClient(client, getParameters());
    }

    public void bind(SourceConfig sourceConfig, RDN pk, String password) throws LDAPException {

        try {
            DN dn = getDn(sourceConfig, pk);

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

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceConfig sourceConfig,
            Filter filter,
            PenroseSearchControls sc,
            Results results
    ) throws LDAPException {

        DNBuilder db = new DNBuilder();
        db.set(sourceConfig.getParameter(BASE_DN));

        String ldapScope = sourceConfig.getParameter(SCOPE);
        String ldapFilter = sourceConfig.getParameter(FILTER);

        db.append(client.getSuffix());
        if (filter != null) {
            ldapFilter = "(&"+ldapFilter+filter+")";

            if (filter instanceof SimpleFilter) {
                db.prepend(filter.toString());
                ldapScope = "OBJECT";
            }
        }

        DN ldapBase = db.toDn();
        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - Base DN: "+ldapBase, 80));
            log.debug(Formatter.displayLine(" - Scope: "+ldapScope, 80));
            log.debug(Formatter.displayLine(" - Filter: "+ldapFilter, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SearchControls ctls = new SearchControls();
        if ("OBJECT".equals(ldapScope)) {
            ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

        } else if ("ONELEVEL".equals(ldapScope)) {
            ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

        } else if ("SUBTREE".equals(ldapScope)) {
            ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        }
        ctls.setCountLimit(sc.getSizeLimit());

        DirContext ctx = null;
        try {
            ctx = ((LDAPClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase.toString(), ldapFilter, ctls);

            log.debug("Result:");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                db.set(sr.getName());
                db.append(ldapBase);
                DN dn = db.toDn();

                log.debug(" - "+dn);

                AttributeValues row = getValues(dn, sourceConfig, sr);
                for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = (Collection)row.get(name);
                    log.debug("   "+name+": "+values);
                }

                ConnectorSearchResult result = new ConnectorSearchResult(row);
                result.setEntryMapping(entryMapping);
                result.setSourceMapping(sourceMapping);
                result.setSourceConfig(sourceConfig);

                results.add(result);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public RDN getPkValues(SourceConfig sourceConfig, javax.naming.directory.SearchResult sr) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        Attributes attrs = sr.getAttributes();
        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = fieldConfig.getName();
            if (name.equals("objectClass")) continue;

            Attribute attr = attrs.get(fieldConfig.getOriginalName());
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

    public AttributeValues getValues(DN dn, SourceConfig sourceConfig, javax.naming.directory.SearchResult sr) throws Exception {

        AttributeValues av = new AttributeValues();

        RDN rdn = dn.getRdn();
        av.add("primaryKey", rdn);

        Attributes attrs = sr.getAttributes();
        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = fieldConfig.getName();
            //if (name.equals("objectClass")) continue;

            Attribute attr = attrs.get(fieldConfig.getOriginalName());
            if (attr == null) continue;

            Collection values = new ArrayList();

            for (NamingEnumeration ne = attr.getAll(); ne.hasMore(); ) {
                Object value = ne.next();
                values.add(value);
            }

            av.add(name, values);
        }

        return av;
    }

    public void add(SourceConfig sourceConfig, RDN pk, AttributeValues sourceValues) throws LDAPException {

        DirContext ctx = null;
        try {
            DN dn = getDn(sourceConfig, pk);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Add "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            Attributes attributes = new BasicAttributes();

            String objectClasses = sourceConfig.getParameter(OBJECT_CLASSES);

            Attribute ocAttribute = new BasicAttribute("objectClass");
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

                Attribute attribute = new BasicAttribute(name);
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
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
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
            Attribute attribute = new BasicAttribute(name);
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
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public void delete(SourceConfig sourceConfig, RDN pk) throws LDAPException {

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
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void modify(SourceConfig sourceConfig, RDN pk, Collection modifications) throws LDAPException {

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
                ModificationItem mi = (ModificationItem)i.next();

                Attribute attribute = mi.getAttribute();
                String name = attribute.getID();

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
                if (fieldConfig == null) continue;

                if ("unicodePwd".equals(name) && mi.getModificationOp() == DirContext.ADD_ATTRIBUTE) { // need to encode unicodePwd
                    Attribute newAttribute = new BasicAttribute(fieldConfig.getOriginalName());
                    for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                        Object value = j.next();
                        newAttribute.add(PasswordUtil.toUnicodePassword(value));
                    }

                    mi = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, newAttribute);

                } else {
                    Attribute newAttribute = new BasicAttribute(fieldConfig.getOriginalName());
                    for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                        Object value = j.next();
                        newAttribute.add(value);
                    }
                    mi = new ModificationItem(mi.getModificationOp(), attribute);
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
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void modrdn(SourceConfig sourceConfig, RDN oldEntry, RDN newRdn) throws LDAPException {

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
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
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

                Attribute attribute = new BasicAttribute(name);
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
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
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

    public PenroseSearchResults getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws LDAPException {

        PenroseSearchResults results = new PenroseSearchResults();

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
                results.add(rdn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw ExceptionUtil.createLDAPException(e);

        } finally {
            try { results.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return results;
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
