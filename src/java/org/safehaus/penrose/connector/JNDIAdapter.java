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
package org.safehaus.penrose.connector;

import javax.naming.directory.*;
import javax.naming.*;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.util.JNDIClient;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.util.PasswordUtil;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JNDIAdapter extends Adapter {

    public final static String BASE_DN = "baseDn";
    public final static String SCOPE   = "scope";
    public final static String FILTER  = "filter";

    private JNDIClient client;

    public void init() throws Exception {
        client = new JNDIClient(getParameters());
    }

    public Object openConnection() throws Exception {
        return new JNDIClient(client, getParameters());
    }

    public int bind(SourceConfig sourceConfig, Row pk, String password) throws Exception {

        String dn = getDn(sourceConfig, pk);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Bind", 80));
            log.debug(Formatter.displayLine(" - Bind DN : "+dn, 80));
            log.debug(Formatter.displayLine(" - Password: "+password, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, getParameter(Context.INITIAL_CONTEXT_FACTORY));
        env.put(Context.PROVIDER_URL, client.getUrl());
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.SECURITY_CREDENTIALS, password);

        try {
            DirContext c = new InitialDirContext(env);
            c.close();
        } catch (AuthenticationException e) {
            log.debug("Error: "+e.getMessage());
            return LDAPException.INVALID_CREDENTIALS;
        }

        return LDAPException.SUCCESS;
    }

    public void search(SourceConfig sourceConfig, Filter filter, long sizeLimit, PenroseSearchResults results) throws Exception {

        String ldapBase = sourceConfig.getParameter(BASE_DN);
        if ("".equals(ldapBase)) {
            ldapBase = client.getSuffix();
        } else {
            ldapBase = ldapBase+","+client.getSuffix();
        }

        String ldapScope = sourceConfig.getParameter(SCOPE);
        String ldapFilter = sourceConfig.getParameter(FILTER);

        if (filter != null) {
            ldapFilter = "(&"+ldapFilter+filter+")";
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - Base DN: "+ldapBase, 80));
            log.debug(Formatter.displayLine(" - Scope: "+ldapScope, 80));
            log.debug(Formatter.displayLine(" - Filter: "+ldapFilter, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SearchControls ctls = new SearchControls();
        ctls.setReturningAttributes(new String[] { "dn" });
        if ("OBJECT".equals(ldapScope)) {
        	ctls.setSearchScope(SearchControls.OBJECT_SCOPE);
        	
        } else if ("ONELEVEL".equals(ldapScope)) {
        	ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
        	
        } else if ("SUBTREE".equals(ldapScope)) {
        	ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        }

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

            log.debug("Result:");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                log.debug(" - "+sr.getName()+","+ldapBase);

                Row row = getPkValues(sourceConfig, sr);
                results.add(row);
            }

        } finally {
            results.close();
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public void load(SourceConfig sourceConfig, Filter filter, long sizeLimit, PenroseSearchResults results) throws Exception {

        String ldapBase = sourceConfig.getParameter(BASE_DN);
        ldapBase = EntryUtil.append(ldapBase, client.getSuffix());

        String ldapScope = sourceConfig.getParameter(SCOPE);
        String ldapFilter = sourceConfig.getParameter(FILTER);

        if (filter != null) {
            ldapFilter = "(&"+ldapFilter+filter+")";
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
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

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

            log.debug("Result:");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                log.debug(" - "+sr.getName()+","+ldapBase);

                AttributeValues av = getValues(sourceConfig, sr);
                results.add(av);
            }

        } finally {
            results.close();
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }

    public AttributeValues get(SourceConfig sourceConfig, Row pk) throws Exception {

        String ldapBase = sourceConfig.getParameter(BASE_DN);
        String dn = EntryUtil.append(pk.toString(), ldapBase);
        dn = EntryUtil.append(dn, client.getSuffix());

        String ldapFilter = sourceConfig.getParameter(FILTER);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - Base DN: "+ldapBase, 80));
            log.debug(Formatter.displayLine(" - Filter: "+ldapFilter, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

            if (!ne.hasMore()) return null;

            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
            log.debug("Result: "+ldapBase);

            return getValues(sourceConfig, sr);

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

    }

    public Row getPkValues(SourceConfig sourceConfig, SearchResult sr) throws Exception {

        Row row = new Row();

        Attributes attrs = sr.getAttributes();
        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = fieldConfig.getName();
            if (name.equals("objectClass")) continue;

            Attribute attr = attrs.get(fieldConfig.getOriginalName());
            if (attr == null) continue;

            Collection values = client.getAttributeValues(attr);
            row.set(name, values.iterator().next());
        }

        return row;
    }

    public AttributeValues getValues(SourceConfig sourceConfig, SearchResult sr) throws Exception {

        AttributeValues av = new AttributeValues();

        Attributes attrs = sr.getAttributes();
        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            
            String name = fieldConfig.getName();
            //if (name.equals("objectClass")) continue;

            Attribute attr = attrs.get(fieldConfig.getOriginalName());
            if (attr == null) continue;

            Collection values = client.getAttributeValues(attr);
            av.add(name, values);
        }

        return av;
    }

    public int add(SourceConfig sourceConfig, AttributeValues entry) throws Exception {

        String dn = getDn(sourceConfig, entry);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Add "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - DN: "+dn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Attributes attrs = new BasicAttributes();

        for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Set set = (Set)entry.get(name);
            if (set.isEmpty()) continue;

            Attribute attr = new BasicAttribute(name);
            for (Iterator j=set.iterator(); j.hasNext(); ) {
                String v = (String)j.next();

                if ("unicodePwd".equals(name)) {
                    attr.add(PasswordUtil.toUnicodePassword(v));
                } else {
                    attr.add(v);
                }
                log.debug(" - "+name+": "+v);
            }
            attrs.put(attr);
        }

        log.debug("Adding "+dn);
        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.createSubcontext(dn, attrs);

        } catch (NameAlreadyBoundException e) {
            return modifyAdd(sourceConfig, entry);
            //log.debug("Error: "+e.getMessage());
            //return LDAPException.ENTRY_ALREADY_EXISTS;
        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modifyDelete(SourceConfig sourceConfig, AttributeValues entry) throws Exception {

        log.debug("JNDI Modify Delete:");

        String dn = getDn(sourceConfig, entry);
        log.debug("Deleting attributes in "+dn);

        List list = new ArrayList();
        Collection fields = sourceConfig.getFieldConfigs();

        for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                if (!fieldConfig.isPrimaryKey()) continue;
                if (!fieldConfig.getName().equals(name)) continue;
                primaryKey = true;
                break;
            }

            if (primaryKey) continue; // don't delete primary key

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
            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.modifyAttributes(dn, mods);
        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int delete(SourceConfig sourceConfig, AttributeValues entry) throws Exception {

        String dn = getDn(sourceConfig, entry);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Delete "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - DN: "+dn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.destroySubcontext(dn);

        } catch (NameNotFoundException e) {
            return LDAPException.NO_SUCH_OBJECT;
        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modify(SourceConfig sourceConfig, AttributeValues oldEntry, AttributeValues newEntry) throws Exception {

        String dn = getDn(sourceConfig, newEntry);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI Modify "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - DN: "+dn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        List list = new ArrayList();
        Collection fields = sourceConfig.getFieldConfigs();

        Set addAttributes = new HashSet(newEntry.getNames());
        addAttributes.removeAll(oldEntry.getNames());
        log.debug("Attributes to add: " + addAttributes);

        Set removeAttributes = new HashSet(oldEntry.getNames());
        removeAttributes.removeAll(newEntry.getNames());
        log.debug("Attributes to remove: " + removeAttributes);

        Set replaceAttributes = new HashSet(oldEntry.getNames());
        replaceAttributes.retainAll(newEntry.getNames());
        log.debug("Attributes to replace: " + replaceAttributes);

        for (Iterator i=addAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if ("objectClass".equals(name)) continue; // don't add objectClass

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                if (!fieldConfig.isPrimaryKey()) continue;
                if (!fieldConfig.getName().equals(name)) continue;
                primaryKey = true;
                break;
            }

            if (primaryKey) continue; // don't add primary key

            Set set = (Set)newEntry.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                String value = (String)j.next();
                log.debug(" - add "+name+": "+value);

                if ("unicodePwd".equals(name)) { // need to encode unicodePwd
                    attribute.add(PasswordUtil.toUnicodePassword(value));
                } else {
                    attribute.add(value);
                }

            }

            if ("unicodePwd".equals(name)) {
                list.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute));
            } else {
                list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute));
            }
        }

        for (Iterator i=removeAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if ("objectClass".equals(name)) continue; // don't remove objectClass
            if ("unicodePwd".equals(name)) continue; // can't remove unicodePwd

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                if (!fieldConfig.isPrimaryKey()) continue;
                if (!fieldConfig.getName().equals(name)) continue;
                primaryKey = true;
                break;
            }

            if (primaryKey) continue; // don't remove primary key

            Set set = (Set)newEntry.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                String value = (String)j.next();
            	log.debug(" - remove "+name+": "+value);

                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attribute));
        }

        for (Iterator i=replaceAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if ("objectClass".equals(name)) continue; // don't replace objectClass

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                if (!fieldConfig.isPrimaryKey()) continue;
                if (!fieldConfig.getName().equals(name)) continue;
                primaryKey = true;
                break;
            }

            if (primaryKey) continue; // don't replace primary key

            Set set = (Set)newEntry.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                String value = (String)j.next();
                log.debug(" - replace "+name+": "+value);

                if ("unicodePwd".equals(name)) { // need to encode unicodePwd
                    attribute.add(PasswordUtil.toUnicodePassword(value));
                } else {
                    attribute.add(value);
                }

            }

            list.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute));
        }

        ModificationItem mods[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.modifyAttributes(dn, mods);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modrdn(SourceConfig sourceConfig, Row oldEntry, Row newEntry) throws Exception {

        String dn = getDn(sourceConfig, oldEntry);
        String newRdn = newEntry.toString();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JNDI ModRDN "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - DN: "+dn, 80));
            log.debug(Formatter.displayLine(" - New RDN: "+newRdn, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.rename(dn, newRdn);
        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modifyAdd(SourceConfig sourceConfig, AttributeValues entry) throws Exception {
        log.debug("JNDI Modify Add:");

        String dn = getDn(sourceConfig, entry);
        log.debug("Replacing attributes "+dn);

        Collection fields = sourceConfig.getFieldConfigs();
        List list = new ArrayList();

        for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Set set = (Set)entry.get(name);
            if (set.isEmpty()) continue;

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                if (!fieldConfig.isPrimaryKey()) continue;
                if (!fieldConfig.getName().equals(name)) continue;
                primaryKey = true;
                break;
            }

            if (primaryKey) continue; // don't add primary key

            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                String v = (String)j.next();
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

        DirContext ctx = null;
        try {
            ctx = ((JNDIClient)openConnection()).getContext();
            ctx.modifyAttributes(dn, mods);
        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public String getDn(SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {
        //log.debug("Computing DN for "+source.getName()+" with "+sourceValues);

        Row pk = sourceConfig.getPrimaryKeyValues(sourceValues);

        String baseDn = sourceConfig.getParameter(BASE_DN);
        //log.debug("Base DN: "+baseDn);

        String dn = EntryUtil.append(pk.toString(), baseDn);
        dn = EntryUtil.append(dn, client.getSuffix());

        //log.debug("DN: "+sb);

        return dn;
    }

    public String getDn(SourceConfig sourceConfig, Row rdn) throws Exception {
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
            log.debug(Formatter.displayLine("JNDI Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
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

                Row row = getPkValues(sourceConfig, sr);
                results.add(row);
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
