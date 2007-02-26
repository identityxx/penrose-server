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
package org.safehaus.penrose.connector;

import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.ietf.ldap.LDAPException;

import javax.naming.*;
import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JNDIAdapter extends Adapter {

    public final static String BASE_DN        = "baseDn";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";

    private JNDIClient client;

    public void init() throws Exception {
        client = new JNDIClient(getParameters());
    }

    public Object openConnection() throws Exception {
        return new JNDIClient(client, getParameters());
    }

    public void bind(SourceConfig sourceConfig, RDN pk, String password) throws LDAPException {

        try {
            String dn = getDn(sourceConfig, pk);

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

        } catch (AuthenticationException e) {
            int rc = LDAPException.INVALID_CREDENTIALS;
            String message = e.getMessage();
            log.debug("Bind failed: "+message);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }

    public void search(
            SourceConfig sourceConfig,
            Filter filter,
            PenroseSearchControls searchControls,
            PenroseSearchResults results
    ) throws Exception {

        String ldapBase = sourceConfig.getParameter(BASE_DN);
        String ldapScope = sourceConfig.getParameter(SCOPE);
        String ldapFilter = sourceConfig.getParameter(FILTER);

        ldapBase = EntryUtil.append(ldapBase, client.getSuffix());
        if (filter != null) {
            ldapFilter = "(&"+ldapFilter+filter+")";
        }

        if (filter instanceof SimpleFilter) {
            SimpleFilter sf = (SimpleFilter)filter;
            ldapBase = EntryUtil.append(sf.toString(), ldapBase);
            ldapScope = "OBJECT";
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Load "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
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
        ctls.setCountLimit(searchControls.getSizeLimit());
        ctls.setTimeLimit(searchControls.getTimeLimit());

        Collection attributes = searchControls.getAttributes();
        ctls.setReturningAttributes((String[])attributes.toArray(new String[attributes.size()]));

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

            log.debug("Result:");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                String dn = EntryUtil.append(sr.getName(), ldapBase);
                log.debug(" - "+dn);

                if (attributes.size() == 1 && attributes.contains("dn")) {
                    RDN rdn = getPkValues(sourceConfig, sr);
                    results.add(rdn);

                } else {
                    AttributeValues av = getValues(dn, sourceConfig, sr);
                    for (Iterator i=av.getNames().iterator(); i.hasNext(); ) {
                        String name = (String)i.next();
                        Collection values = (Collection)av.get(name);
                        log.debug("   "+name+": "+values);
                    }

                    results.add(av);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(ExceptionUtil.getReturnCode(e));

        } finally {
            results.close();
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public RDN getPkValues(SourceConfig sourceConfig, SearchResult sr) throws Exception {

        RDN rdn = new RDN();

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

            rdn.set(name, values.iterator().next());
        }

        return rdn;
    }

    public AttributeValues getValues(String dn, SourceConfig sourceConfig, SearchResult sr) throws Exception {

        AttributeValues av = new AttributeValues();

        RDN rdn = EntryUtil.getRdn(dn);
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
            String dn = getDn(sourceConfig, pk);

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

            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.createSubcontext(dn, attributes);

        } catch (NameAlreadyBoundException e) {
            modifyAdd(sourceConfig, sourceValues);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public int modifyDelete(SourceConfig sourceConfig, AttributeValues entry) throws LDAPException {

        log.debug("Modify Delete:");

        DirContext ctx = null;
        try {
            String dn = getDn(sourceConfig, entry);
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

            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.modifyAttributes(dn, mods);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public void delete(SourceConfig sourceConfig, RDN pk) throws LDAPException {

        DirContext ctx = null;
        try {
            String dn = getDn(sourceConfig, pk);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Delete "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.destroySubcontext(dn);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void modify(SourceConfig sourceConfig, RDN pk, Collection modifications) throws LDAPException {

        DirContext ctx = null;
        try {
            String dn = getDn(sourceConfig, pk);

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

            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.modifyAttributes(dn, mods);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void modrdn(SourceConfig sourceConfig, RDN oldEntry, RDN newEntry) throws LDAPException {

        DirContext ctx = null;
        try {
            String dn = getDn(sourceConfig, oldEntry);
            String newRdn = newEntry.toString();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("ModRDN "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
                log.debug(Formatter.displayLine(" - DN: "+dn, 80));
                log.debug(Formatter.displayLine(" - New RDN: "+newRdn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.rename(dn, newRdn);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void modifyAdd(SourceConfig sourceConfig, AttributeValues entry) throws LDAPException {
        log.debug("Modify Add:");

        DirContext ctx = null;
        try {
            String dn = getDn(sourceConfig, entry);
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

            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.modifyAttributes(dn, mods);

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public String getDn(SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {
        //log.debug("Computing DN for "+source.getName()+" with "+sourceValues);

        RDN pk = sourceConfig.getPrimaryKeyValues(sourceValues);
        //RDN pk = sourceValues.getRdn();

        String baseDn = sourceConfig.getParameter(BASE_DN);
        //log.debug("Base DN: "+baseDn);

        String dn = EntryUtil.append(pk.toString(), baseDn);
        dn = EntryUtil.append(dn, client.getSuffix());

        //log.debug("DN: "+sb);

        return dn;
    }

    public String getDn(SourceConfig sourceConfig, RDN rdn) throws Exception {
        String baseDn = sourceConfig.getParameter(BASE_DN);
        //log.debug("Base DN: "+baseDn);

        String dn = EntryUtil.append(rdn.toString(), baseDn);
        dn = EntryUtil.append(dn, client.getSuffix());

        //log.debug("DN: "+sb);

        return dn;
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws Exception {
        return 0;
    }

    public PenroseSearchResults getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws Exception {

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
            ctx = ((JNDIClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

            log.debug("Result:");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                log.debug(" - "+sr.getName()+","+ldapBase);

                RDN rdn = getPkValues(sourceConfig, sr);
                results.add(rdn);
            }

        } finally {
            results.close();
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

    public JNDIClient getClient() throws Exception {
        return new JNDIClient(client, getParameters());
    }
}
