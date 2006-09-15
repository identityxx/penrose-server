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
package org.safehaus.penrose.test.ldap;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
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
public class AuthenticatedAccessTest extends TestCase {

    PenroseConfig penroseConfig;
    PenroseServer penroseServer;

    static {
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.DEBUG);
    }

    public void setUp() throws Exception {

        penroseConfig = new DefaultPenroseConfig();

        penroseServer = new PenroseServer(penroseConfig);
        penroseServer.start();

    }

    public void tearDown() throws Exception {
        penroseServer.stop();
    }

    public void testAuthenticatedAccessWithJNDIClient() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.put(Context.SECURITY_CREDENTIALS, "secret");

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

    public void testAuthenticatedAccessWithV2JNDIClient() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.put(Context.SECURITY_CREDENTIALS, "secret");
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

    public void testAuthenticatedAccessWithV3JNDIClient() throws Exception {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.put(Context.SECURITY_CREDENTIALS, "secret");
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

    public void testAuthenticatedAccessWithV2LDAPClient() throws Exception {
        LDAPConnection connection = new LDAPConnection();
        connection.connect("localhost", 10389);

        connection.bind(2, "uid=admin,ou=system", "secret".getBytes());

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

    public void testAuthenticatedAccessWithV3LDAPClient() throws Exception {
        LDAPConnection connection = new LDAPConnection();
        connection.connect("localhost", 10389);

        connection.bind(3, "uid=admin,ou=system", "secret".getBytes());

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
