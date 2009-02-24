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
package org.safehaus.penrose.test.partition;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.partition.*;

/**
 * @author Endi S. Dewata
 */
public class PartitionManagerTest extends TestCase {

    PenroseConfig penroseConfig;
    Penrose penrose;

    public void setUp() throws Exception {

        //PatternLayout patternLayout = new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n");
        PatternLayout patternLayout = new PatternLayout("%-20C{1} [%4L] %m%n");

        ConsoleAppender appender = new ConsoleAppender(patternLayout);
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.DEBUG);
        logger.setAdditivity(false);

        penroseConfig = new DefaultPenroseConfig();
        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);

    }

    public void tearDown() throws Exception {
    }

    public void testAddingPartition() throws Exception {

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose();
        penrose.start();

        PartitionConfig partitionConfig = new PartitionConfig();
        partitionConfig.setName(PartitionConfig.ROOT);

        EntryConfig entryConfig = new EntryConfig();
        entryConfig.setDn("ou=Test,dc=Example,dc=com");
        entryConfig.addObjectClass("organizationalUnit");
        entryConfig.addAttributesFromRdn();
        partitionConfig.getDirectoryConfig().addEntryConfig(entryConfig);

        PartitionManager partitionManager = penrose.getPartitionManager();
        partitionManager.createPartition(partitionConfig);

        Session session = penrose.createSession();
        session.setBindDn("uid=admin,ou=system");

        SearchResponse response = session.search(
                "ou=Test,dc=Example,dc=com",
                "(objectClass=*)",
                SearchRequest.SCOPE_BASE
        );

        assertTrue(response.hasNext());

        SearchResult searchResult = response.next();
        DN dn = searchResult.getDn();
        assertTrue(dn.matches("ou=Test,dc=Example,dc=com"));

        penrose.stop();
    }
/*
    public void testSearchingPartition() throws Exception {

        penrose.stop();

        PartitionConfig partitionConfig = new PartitionConfig("example", "samples/shop/partition");
        penroseConfig.addPartitionConfig(partitionConfig);

        PartitionReader partitionReader = new PartitionReader();
        Partition partition = partitionReader.read(partitionConfig);

        PartitionManager partitionManager = penrose.getPartitionConfigManager();
        partitionManager.addPartition(partition);

        partitionManager.findPartition("dc=Shop,c=Example,dc=com");
    }

    public int search() throws Exception {

        Session session = penrose.createSession();
        session.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());

        SearchResponse results = new SearchResponse();

        SearchRequest sc = new SearchRequest();
        sc.setScope(SearchRequest.SCOPE_ONE);

        String baseDn = "ou=Categories,dc=Shop,dc=Example,dc=com";

        System.out.println("Searching "+baseDn+":");
        session.search(baseDn, "(objectClass=*)", sc, results);

        for (Iterator i = results.iterator(); i.hasNext();) {
            SearchResult entry = (SearchResult)i.next();
            System.out.println("dn: "+entry.getName());
        }

        session.unbind();

        session.close();

        return results.getReturnCode();
    }
*/
}
