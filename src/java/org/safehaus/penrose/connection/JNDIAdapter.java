/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.connection;


import javax.naming.directory.*;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.*;
import java.util.*;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.Field;
import org.safehaus.penrose.SearchResults;

/**
 * @author Endi S. Dewata
 */
public class JNDIAdapter extends Adapter {

    public final static String BASE_DN = "baseDn";
    public final static String SCOPE   = "scope";
    public final static String FILTER  = "filter";

    public DirContext ctx;

    public void init() throws Exception {
        String name = getConnectionName();

    	log.debug("-------------------------------------------------------------------------------");
    	log.debug("Initializing JNDI connection "+name+":");

        Hashtable env = new Hashtable();
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String param = (String)i.next();
            String value = getParameter(param);
            log.debug(param+": "+value);
            env.put(param, value);
        }

        ctx = new InitialDirContext(env);
    }

    public SearchResults search(Source source, Filter filter, long sizeLimit) throws Exception {

        log.debug("JNDI Search:");

        SearchResults results = new SearchResults();

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("JNDI Source: "+source.getConnectionName());

        String ldapBase = source.getParameter(BASE_DN);
        String ldapScope = source.getParameter(SCOPE);
        String ldapFilter = source.getParameter(FILTER);

        if (filter != null) {
            ldapFilter = "(&"+ldapFilter+filter+")";
        }

        log.debug("base: "+ldapBase);
        log.debug("filter: "+ldapFilter);

        SearchControls ctls = new SearchControls();
        if ("OBJECT".equals(ldapScope)) {
        	ctls.setSearchScope(SearchControls.OBJECT_SCOPE);
        	
        } else if ("ONELEVEL".equals(ldapScope)) {
        	ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
        	
        } else if ("SUBTREE".equals(ldapScope)) {
        	ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        }

        NamingEnumeration ne = ctx.search(ldapBase, ldapFilter, ctls);

        log.debug("Result:");

        while (ne.hasMore()) {
            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
            log.debug(" - "+sr.getName()+","+ldapBase);

            Collection rows = getRows(source, sr);
            results.addAll(rows);
        }

        results.close();
        
        return results;
    }

    public Collection getRows(Source source, javax.naming.directory.SearchResult sr) throws Exception {

        org.safehaus.penrose.mapping.AttributeValues map = new org.safehaus.penrose.mapping.AttributeValues();

        Attributes attrs = sr.getAttributes();
        Collection fields = source.getFields();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            String name = field.getName();
            if (name.equals("objectClass")) continue;

            javax.naming.directory.Attribute attr = attrs.get(field.getName());
            if (attr == null) continue;

            boolean binary = false;
            try {
                attr.getAttributeSyntaxDefinition();
            } catch (Exception e) {
                binary = "SyntaxDefinition/1.3.6.1.4.1.1466.115.121.1.40".equals(e.getMessage());
            }

            Set set = (Set)map.get(name);

            if (set == null) {
                set = new HashSet();
                map.set(name, set);
            }

            NamingEnumeration attributeValues = attr.getAll();
            while (attributeValues.hasMore()) {
                Object value = attributeValues.next();
                set.add(value);
            }
        }

        Collection rows = getAdapterContext().getTransformEngine().convert(map);

        List list = new ArrayList();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            Row values = new Row();

            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                String name = field.getName();
                values.set(name, row.get(field.getOriginalName()));
            }

            //log.debug(" - "+values);
            list.add(values);
        }

        return list;
    }

    public int bind(Source source, org.safehaus.penrose.mapping.AttributeValues values, String password) throws Exception {

        log.debug("JNDI Bind:");

        if (!values.contains("objectClass")) {
            return LDAPException.INVALID_CREDENTIALS;
        }

        String url = getParameter(Context.PROVIDER_URL);
        int i = url.indexOf("://");
        int j = url.indexOf("/", i+3);
        String suffix = url.substring(j+1);

        String dn = getDn(source, values)+","+suffix;
        log.debug("Binding as "+dn);

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, getParameter(Context.INITIAL_CONTEXT_FACTORY));
        env.put(Context.PROVIDER_URL, url);
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

    public int add(Source source, org.safehaus.penrose.mapping.AttributeValues entry) throws Exception {
        log.debug("JNDI Add:");

        if (!entry.contains("objectClass")) {
            return modifyAdd(source, entry);
        }

        String dn = getDn(source, entry);

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
        try {
            ctx.createSubcontext(dn, attrs);
        } catch (NameAlreadyBoundException e) {
            log.debug("Error: "+e.getMessage());
            return LDAPException.ENTRY_ALREADY_EXISTS;
        }

        return LDAPException.SUCCESS;
    }

    public int modifyDelete(Source source, org.safehaus.penrose.mapping.AttributeValues entry) throws Exception {
        log.debug("JNDI Modify Delete:");

        String dn = getDn(source, entry);
        log.debug("Deleting attributes in "+dn);

        List list = new ArrayList();
        Collection fields = source.getPrimaryKeyFields();

        for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                if (!field.getName().equals(name)) continue;
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

        ctx.modifyAttributes(dn, mods);

        return LDAPException.SUCCESS;
    }

    public int delete(Source source, org.safehaus.penrose.mapping.AttributeValues entry) throws Exception {
        log.debug("JNDI Delete:");

        if (!entry.contains("objectClass")) {
            return modifyDelete(source, entry);
        }

        String dn = getDn(source, entry);
        log.debug("Deleting entry "+dn);

        try {
            ctx.destroySubcontext(dn);
            
        } catch (NameNotFoundException e) {
            return LDAPException.NO_SUCH_OBJECT;
        }

        return LDAPException.SUCCESS;
    }

    public int modify(Source source, org.safehaus.penrose.mapping.AttributeValues oldEntry, org.safehaus.penrose.mapping.AttributeValues newEntry) throws Exception {
        log.debug("JNDI Modify:");

        String dn = getDn(source, newEntry);
        log.debug("Replacing attributes "+dn);

        List list = new ArrayList();
        Collection fields = source.getPrimaryKeyFields();

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
                Field field = (Field)j.next();
                if (!field.getName().equals(name)) continue;
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

            list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute));
        }

        for (Iterator i=removeAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if ("objectClass".equals(name)) continue; // don't remove objectClass
            if ("unicodePwd".equals(name)) continue; // can't remove unicodePwd

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                if (!field.getName().equals(name)) continue;
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
                Field field = (Field)j.next();
                if (!field.getName().equals(name)) continue;
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

        ctx.modifyAttributes(dn, mods);

        return LDAPException.SUCCESS;
    }

    public int modifyAdd(Source source, org.safehaus.penrose.mapping.AttributeValues entry) throws Exception {
        log.debug("JNDI Modify Add:");

        String dn = getDn(source, entry);
        log.debug("Replacing attributes "+dn);

        Collection fields = source.getPrimaryKeyFields();
        List list = new ArrayList();

        for (Iterator i=entry.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Set set = (Set)entry.get(name);
            if (set.isEmpty()) continue;

            boolean primaryKey = false;
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                if (!field.getName().equals(name)) continue;
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
        ctx.modifyAttributes(dn, mods);

        return LDAPException.SUCCESS;
    }

    public String getDn(Source source, org.safehaus.penrose.mapping.AttributeValues columnValues) throws Exception {
        String baseDn = source.getParameter(BASE_DN);

        Collection fields= source.getFields();
        StringBuffer sb = new StringBuffer();

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            if (!field.isPrimaryKey()) continue;

            if (sb.length() > 0) sb.append("+");

            String name = field.getName();
            sb.append(name);
            sb.append("=");

            Object value = columnValues.get(name);
            if (value instanceof Collection) {
                Collection c = (Collection)value;
                value = c.iterator().next();
            }

            sb.append(value);
        }

        return sb+","+baseDn;
    }
}
