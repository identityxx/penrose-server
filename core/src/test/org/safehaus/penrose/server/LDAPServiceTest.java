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
package org.safehaus.penrose.server;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.ldap.PenroseLDAPService;
import org.safehaus.penrose.service.ServiceManager;

import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.NamingEnumeration;
import javax.naming.Context;
import java.util.Hashtable;

/**
 * @author Endi S. Dewata
 */
public class LDAPServiceTest extends TestCase {

    PenroseConfig penroseConfig;
    PenroseServer penroseServer;

    public void setUp() throws Exception {

        //PatternLayout patternLayout = new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n");
        PatternLayout patternLayout = new PatternLayout("%-20C{1} [%4L] %m%n");

        ConsoleAppender appender = new ConsoleAppender(patternLayout);
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger ldapLogger = Logger.getLogger("org.apache");
        ldapLogger.setLevel(Level.INFO);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.INFO);

        penroseConfig = new DefaultPenroseConfig();
        penroseConfig.removeServiceConfig("JMX");

        SchemaConfig schemaConfig = new SchemaConfig("samples/shop/schema/example.schema");
        penroseConfig.addSchemaConfig(schemaConfig);

        PartitionConfig partitionConfig = new PartitionConfig("example", "samples/shop/partition");
        penroseConfig.addPartitionConfig(partitionConfig);

        penroseServer = new PenroseServer(penroseConfig);
        penroseServer.start();

    }

    public void tearDown() throws Exception {
        penroseServer.stop();
    }

    public void testBasicSearch() throws Exception {
        ServiceManager serviceManager = penroseServer.getServiceManager();
        PenroseLDAPService service = (PenroseLDAPService)serviceManager.getService("LDAP");
        int port = service.getLdapPort();
        search(port);
    }

    public void testRestartingLDAPService() throws Exception {

        ServiceManager serviceManager = penroseServer.getServiceManager();
        PenroseLDAPService service = (PenroseLDAPService)serviceManager.getService("LDAP");
        int port = service.getLdapPort();

        try {
            search(port+1);
            fail("Searching should have failed.");
        } catch (Exception e) {
            System.out.println("Searching failed as expected: "+e.getMessage());
        }

        try {
            search(port);
            System.out.println("Searching succeeded as expected.");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Searching should have succeeded.");
        }

        penroseServer.stop();

        try {
            search(port);
            fail("Searching should have failed.");
        } catch (Exception e) {
            System.out.println("Searching failed as expected: "+e.getMessage());
        }

        penroseServer.start();

        try {
            search(port);
            System.out.println("Searching succeeded as expected.");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Searching should have succeeded.");
        }

        penroseServer.stop();

        try {
            search(port);
            fail("Searching should have failed.");
        } catch (Exception e) {
            System.out.println("Searching failed as expected: "+e.getMessage());
        }

        penroseServer.start();

        try {
            search(port);
            System.out.println("Searching succeeded as expected.");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Searching should have succeeded.");
        }
    }

    public void testChangingLDAPPort() throws Exception {

        ServiceManager serviceManager = penroseServer.getServiceManager();
        PenroseLDAPService service = (PenroseLDAPService)serviceManager.getService("LDAP");
        int port = service.getLdapPort();

        // testing the old port
        try {
            search(port);
            System.out.println("Searching at the old port succeeded as expected.");
        } catch (Exception e) {
            fail("Searching at the old port should have succeeded.");
        }

        // switching to the new port
        penroseServer.stop();

        service.setLdapPort(port+1);

        penroseServer.start();

        try {
            search(port);
            fail("Searching at the old port should have failed.");
        } catch (Exception e) {
            System.out.println("Searching at the old port failed as expected.");
        }

        try {
            search(port+1);
            System.out.println("Searching at the new port succeeded as expected.");
        } catch (Exception e) {
            fail("Searching at the new port should have succeeded.");
        }

        // switching back to the old port
        penroseServer.stop();

        service.setLdapPort(port);

        penroseServer.start();

        try {
            search(port);
            System.out.println("Searching at the old port succeeded.");
        } catch (Exception e) {
            fail("Searching at the old port should have succeeded as expected.");
        }

        try {
            search(port+1);
            fail("Searching at the new port should have failed.");
        } catch (Exception e) {
            System.out.println("Searching at the old port failed as expected.");
        }
    }

    public void search(int port) throws Exception {

        System.out.println("Searching at port "+port);

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:"+port);
        env.put(Context.SECURITY_PRINCIPAL, penroseConfig.getRootUserConfig().getDn());
        env.put(Context.SECURITY_CREDENTIALS, penroseConfig.getRootUserConfig().getPassword());

        DirContext ctx = new InitialDirContext(env);

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

        String baseDn = "ou=Categories,dc=Shop,dc=Example,dc=com";

        NamingEnumeration ne = ctx.search(baseDn, "(objectClass=*)", ctls);

        if (ne.hasMore()) {
            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                String dn = "".equals(sr.getName()) ? baseDn : sr.getName()+","+baseDn;
                System.out.println("dn: "+dn);
            }
        } else {
            System.out.println("No results found.");
        }

        ctx.close();
    }
}
