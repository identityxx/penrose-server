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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.filter.Filter;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PersistentEngineCache extends EngineCache {

    Hashtable env;

    public String url;
    public String suffix;

    public void init() throws Exception {
        super.init();

        //log.debug("-------------------------------------------------------------------------------");
        //log.debug("Initializing PersistentEngineCache:");

        env = new Hashtable();
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String param = (String)i.next();
            String value = getParameter(param);
            //log.debug(" - "+param+": "+value);

            if (param.equals(Context.PROVIDER_URL)) {

                int index = value.indexOf("://");
                index = value.indexOf("/", index+3);
                if (index >= 0) { // extract suffix from url
                    suffix = value.substring(index+1);
                    url = value.substring(0, index);
                } else {
                    suffix = "";
                    url = value;
                }
                env.put(param, url);

            } else {
                env.put(param, value);
            }
        }

        env.put("com.sun.jndi.ldap.connect.pool", "true");
    }

    public void create() throws Exception {

        String dn = entryDefinition.getDn();
        //log.debug("Adding "+dn);

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();
        AttributeValues attributeValues = engine.computeAttributeValues(entryDefinition, interpreter);
        interpreter.clear();

        Entry entry = new Entry(dn, entryDefinition, attributeValues);
        Row rdn = entry.getRdn();

        put(rdn, entry);
    }

    public void clean() throws Exception {
        String dn = entryDefinition.getDn();
        //log.debug("Deleting "+dn);

        Row rdn = Entry.getRdn(dn);
        remove(rdn);
    }

    public void load() throws Exception {

        Config config = engine.getConfig(entryDefinition.getDn());
        Collection entries = config.getChildren(entryDefinition);
        load(config, entries);
    }

    public void load(Config config, Collection entries) throws Exception {
        if (entries == null) return;

        for (Iterator i = entries.iterator(); i.hasNext();) {
            EntryDefinition ed = (EntryDefinition) i.next();
            String dn = ed.getDn();

            engine.search(null, new AttributeValues(), ed, null, null);

            Collection children = config.getChildren(ed);
            load(config, children);
        }
    }

    public Object get(Object pk) throws Exception {
        Row rdn = (Row)pk;
        String dn = rdn+","+parentDn;
        try {
            log.debug("Getting "+dn);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.OBJECT_SCOPE);

            DirContext ctx = new InitialDirContext(env);
            NamingEnumeration ne = ctx.search(dn, "(objectClass=*)", sc);

            if (!ne.hasMore()) return null;

            SearchResult sr = (SearchResult)ne.next();
            log.debug("Found:");

            Attributes attributes = sr.getAttributes();
            AttributeValues attributeValues = new AttributeValues();
            for (NamingEnumeration ne2 = attributes.getAll(); ne2.hasMore(); ) {
                Attribute attribute = (Attribute)ne2.next();
                String name = attribute.getID();

                for (NamingEnumeration ne3 = attribute.getAll(); ne3.hasMore(); ) {
                    Object value = ne3.next();
                    log.debug(" - "+name+": "+value);
                    attributeValues.add(name, value);
                }
            }

            Entry entry = new Entry(dn, entryDefinition, attributeValues);
            return entry;

        } catch (NamingException e) {
            log.debug("Error: "+e.getMessage());
        }
        return null;
    }

    public Collection get(Filter filter) throws Exception {
        Collection results = new ArrayList();
        try {
            log.debug("Searching "+filter);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            sc.setReturningAttributes(new String[] { "dn" });

            DirContext ctx = new InitialDirContext(env);
            NamingEnumeration ne = ctx.search(parentDn, filter.toString(), sc);

            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                String dn = sr.getName()+","+parentDn;
                Attributes attributes = sr.getAttributes();

                log.debug(" - "+dn);
                results.add(dn);
            }

        } catch (NamingException e) {
            log.debug("Error: "+e.getMessage());
        }

        return results;
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }

    public Map search(Collection filters) throws Exception {

        Map values = new TreeMap();

        return values;
    }

    public void put(Object key, Object object) throws Exception {

        Entry entry = (Entry)object;
        String dn = entry.getDn();

        log.debug("Storing "+dn);

        AttributeValues attributeValues = entry.getAttributeValues();

        Attributes attrs = new BasicAttributes();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);
            if (values.isEmpty()) continue;

            Attribute attr = new BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                log.debug(" - "+name+": "+value);

                if ("unicodePwd".equals(name)) {
                    attr.add(PasswordUtil.toUnicodePassword(value.toString()));
                } else {
                    attr.add(value.toString());
                }
            }
            attrs.put(attr);
        }

        try {
            DirContext ctx = new InitialDirContext(env);
            ctx.createSubcontext(dn, attrs);
        } catch (NamingException e) {
            log.debug("Error: "+e.getMessage());
        }
    }

    public void remove(Object key) throws Exception {
        Row rdn = (Row)key;
        String dn = parentDn == null ? entryDefinition.getDn() : rdn+","+parentDn;
        try {
            log.debug("Removing "+dn);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sc.setReturningAttributes(new String[] { "dn" });

            DirContext ctx = new InitialDirContext(env);
            NamingEnumeration ne = ctx.search(dn, "(objectClass=*)", sc);

            ArrayList dns = new ArrayList();
            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                String rdn2 = sr.getName();
                String dn2 = "".equals(rdn2) ? dn : rdn2+","+dn;
                dns.add(0, dn2);
            }

            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                String dn2 = (String)i.next();
                log.debug(" - "+dn2);

                ctx.destroySubcontext(dn2);
            }

        } catch (NamingException e) {
            log.debug("Error: "+e.getMessage());
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
