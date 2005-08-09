/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.apacheds;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.cache.CacheContext;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;
import org.apache.ldap.server.interceptor.NextInterceptor;
import org.apache.ldap.server.interceptor.InterceptorChain;
import org.apache.ldap.server.invocation.InvocationStack;
import org.apache.ldap.server.invocation.Invocation;
import org.apache.ldap.server.jndi.ServerLdapContext;
import org.apache.ldap.server.jndi.ContextFactoryService;
import org.apache.ldap.server.authn.LdapPrincipal;
import org.apache.ldap.common.name.LdapName;
import org.apache.ldap.common.filter.FilterParserImpl;
import org.apache.ldap.common.filter.ExprNode;

import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import javax.naming.Context;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ApacheDSEntryCache extends EntryCache {

    private NextInterceptor nextInterceptor;
    private Context context;

    private Map entries = new HashMap();

    public void init() throws Exception {
    }

    public Entry get(String dn) throws Exception {

        log.debug("===============================================================================");
        log.debug("Getting entry cache ("+entries.size()+"): "+dn);
        log.debug("===============================================================================");

        LdapName baseDn = new LdapName(dn);
        //LdapName baseDn = new LdapName("dc=vergenet,dc=com");
        Map env = new HashMap();

        FilterParserImpl parser = new FilterParserImpl();
        String originalFilter = "(objectClass=*)";
        ExprNode filter = parser.parse( originalFilter );

        SearchControls searchControls = new SearchControls();

        InvocationStack stack = InvocationStack.getInstance();
        stack.push( new Invocation(
                context, "search",
                new Object[] { baseDn, env, filter, searchControls } ) );

        try {
            NamingEnumeration ne = nextInterceptor.search(baseDn, env, filter, searchControls);
            ne.hasMore();

            SearchResult sr = (SearchResult)ne.next();
            log.debug("dn: "+sr.getName());

            Attributes attributes = sr.getAttributes();
            AttributeValues attributeValues = new AttributeValues();

            for (NamingEnumeration ne2 = attributes.getAll(); ne2.hasMore(); ) {
                Attribute attribute = (Attribute)ne2.next();
                String name = attribute.getID();

                for (NamingEnumeration ne3 = attribute.getAll(); ne3.hasMore(); ) {
                    Object value = ne3.next();
                    log.debug(name+": "+value);
                    attributeValues.add(name, value);
                }
            }

            Entry entry = new Entry(null, attributeValues);
            return entry;

        } catch (Exception e) {
            log.debug("Error: "+e.getMessage());
            return null;

        } finally {
            stack.pop();
        }
/*
        Entry entry = (Entry)entries.remove(dn);
        entries.put(dn, entry);

        return entry;
*/
    }

    public void put(Entry entry) throws Exception {

        String dn = entry.getDn();

        while (entries.size() >= getSize()) {
            log.debug("Trimming entry cache ("+entries.size()+").");
            Row key = (Row)entries.keySet().iterator().next();
            entries.remove(key);
        }

        log.debug("Storing entry cache ("+entries.size()+"): "+dn);
        entries.put(dn, entry);
    }

    public void remove(Entry entry) throws Exception {

        String dn = entry.getDn();

        log.debug("Removing entry cache ("+entries.size()+"): "+dn);
        entries.remove(dn);
    }

    public Map getEntries() {
        return entries;
    }

    public void setEntries(Map entries) {
        this.entries = entries;
    }

    public NextInterceptor getNextInterceptor() {
        return nextInterceptor;
    }

    public void setNextInterceptor(NextInterceptor nextInterceptor) {
        this.nextInterceptor = nextInterceptor;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }
}
