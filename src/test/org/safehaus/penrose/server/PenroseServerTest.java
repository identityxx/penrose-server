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
import org.safehaus.penrose.PenroseServer;

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
public class PenroseServerTest extends TestCase {

    PenroseConfig penroseConfig;
    PenroseServer penroseServer;

    public void setUp() throws Exception {

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.toLevel("OFF"));

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.toLevel("INFO"));

        penroseConfig = new DefaultPenroseConfig();

        SchemaConfig schemaConfig = new SchemaConfig("samples/schema/example.schema");
        penroseConfig.addSchemaConfig(schemaConfig);

        PartitionConfig partitionConfig = new PartitionConfig("example", "samples/conf");
        penroseConfig.addPartitionConfig(partitionConfig);

        penroseServer = new PenroseServer(penroseConfig);
        penroseServer.start();

    }

    public void tearDown() throws Exception {
        penroseServer.stop();
    }

    public void testLDAPService() throws Exception {
        performSearch();
    }

    public void testLDAPServiceOnDifferentPort() throws Exception {
        penroseServer.stop();
        penroseConfig.setPort(penroseConfig.getPort()+1);
        penroseServer.start();

        performSearch();

        penroseServer.stop();
        penroseConfig.setPort(penroseConfig.getPort()-1);
        penroseServer.start();

        performSearch();
    }

    public void performSearch() throws Exception {

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, "ldap://localhost:"+penroseConfig.getPort());
        env.put(Context.SECURITY_PRINCIPAL, penroseConfig.getRootDn());
        env.put(Context.SECURITY_CREDENTIALS, penroseConfig.getRootPassword());

        DirContext ctx = new InitialDirContext(env);

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

        String baseDn = "ou=Categories,dc=Example,dc=com";

        System.out.println("Searching "+baseDn+" at port "+penroseConfig.getPort());
        NamingEnumeration ne = ctx.search(baseDn, "(objectClass=*)", ctls);

        while (ne.hasMore()) {
            SearchResult sr = (SearchResult)ne.next();
            String dn = "".equals(sr.getName()) ? baseDn : sr.getName()+","+baseDn;
            System.out.println("dn: "+dn);
        }

        ctx.close();
    }
}
