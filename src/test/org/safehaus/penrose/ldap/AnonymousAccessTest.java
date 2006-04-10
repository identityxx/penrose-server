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
package org.safehaus.penrose.ldap;

import junit.framework.TestCase;
import org.ietf.ldap.*;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * @author Endi S. Dewata
 */
public class AnonymousAccessTest extends TestCase {

    /**
     *  Search Root DSE anonymously using JNDI client without specifying LDAP protocol version.
     *  The behavior is defined in http://java.sun.com/products/jndi/tutorial/ldap/misc/version.html.
     */
    public void testAnonymousAccessWithJNDIClient() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");

        DirContext ctx = new InitialDirContext(env);

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.OBJECT_SCOPE);
        NamingEnumeration e = ctx.search("", "(objectClass=*)", sc);

        assertTrue(e.hasMore());

        System.out.println("Root DSE:");
        SearchResult sr = (SearchResult)e.next();
        for (NamingEnumeration i=sr.getAttributes().getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                String value = (String)j.nextElement();
                System.out.println(" - "+name+": "+value);
            }
        }

        ctx.close();
    }

    /**
     *  Search Root DSE anonymously using JNDI client with LDAP protocol version 2.
     */
    public void testAnonymousAccessWithV2JNDIClient() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put("java.naming.ldap.version", "2");

        DirContext ctx = new InitialDirContext(env);

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.OBJECT_SCOPE);
        NamingEnumeration e = ctx.search("", "(objectClass=*)", sc);

        assertTrue(e.hasMore());

        System.out.println("Root DSE:");
        SearchResult sr = (SearchResult)e.next();
        for (NamingEnumeration i=sr.getAttributes().getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                String value = (String)j.nextElement();
                System.out.println(" - "+name+": "+value);
            }
        }

        ctx.close();
    }

    /**
     *  Search Root DSE anonymously using JNDI client with LDAP protocol version 3.
     */
    public void testAnonymousAccessWithV3JNDIClient() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put("java.naming.ldap.version", "3");

        DirContext ctx = new InitialDirContext(env);

        SearchControls sc = new SearchControls();
        sc.setSearchScope(SearchControls.OBJECT_SCOPE);
        NamingEnumeration e = ctx.search("", "(objectClass=*)", sc);

        assertTrue(e.hasMore());

        System.out.println("Root DSE:");
        SearchResult sr = (SearchResult)e.next();
        for (NamingEnumeration i=sr.getAttributes().getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                String value = (String)j.nextElement();
                System.out.println(" - "+name+": "+value);
            }
        }

        ctx.close();
    }

    /**
     *  Search Root DSE anonymously using Java LDAP client without bind.
     */
    public void testAnonymousAccessWithLDAPClient() throws Exception {
        LDAPConnection connection = new LDAPConnection();
        connection.connect("localhost", 10389);

        LDAPSearchResults results = connection.search("", LDAPConnection.SCOPE_BASE, "(objectClass=*)", new String[0], false);

        assertTrue(results.hasMore());

        System.out.println("Root DSE:");
        LDAPEntry entry = results.next();
        for (Iterator i=entry.getAttributeSet().iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            String name = attribute.getName();
            for (Enumeration j=attribute.getStringValues(); j.hasMoreElements(); ) {
                String value = (String)j.nextElement();
                System.out.println(" - "+name+": "+value);
            }
        }

        connection.disconnect();
    }

    /**
     *  Search Root DSE anonymously using Java LDAP client with empty bind with LDAP protocol version 2.
     */
    public void testAnonymousAccessWithV2LDAPClient() throws Exception {
        LDAPConnection connection = new LDAPConnection();
        connection.connect("localhost", 10389);

        connection.bind(2, null, null);

        LDAPSearchResults results = connection.search("", LDAPConnection.SCOPE_BASE, "(objectClass=*)", new String[0], false);

        assertTrue(results.hasMore());

        System.out.println("Root DSE:");
        LDAPEntry entry = results.next();
        for (Iterator i=entry.getAttributeSet().iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            String name = attribute.getName();
            for (Enumeration j=attribute.getStringValues(); j.hasMoreElements(); ) {
                String value = (String)j.nextElement();
                System.out.println(" - "+name+": "+value);
            }
        }

        connection.disconnect();
    }

    /**
     *  Search Root DSE anonymously using Java LDAP client with empty bind with LDAP protocol version 3.
     */
    public void testAnonymousAccessWithV3LDAPClient() throws Exception {
        LDAPConnection connection = new LDAPConnection();
        connection.connect("localhost", 10389);

        connection.bind(3, null, null);

        LDAPSearchResults results = connection.search("", LDAPConnection.SCOPE_BASE, "(objectClass=*)", new String[0], false);

        assertTrue(results.hasMore());

        System.out.println("Root DSE:");
        LDAPEntry entry = results.next();
        for (Iterator i=entry.getAttributeSet().iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            String name = attribute.getName();
            for (Enumeration j=attribute.getStringValues(); j.hasMoreElements(); ) {
                String value = (String)j.nextElement();
                System.out.println(" - "+name+": "+value);
            }
        }

        connection.disconnect();
    }
}
