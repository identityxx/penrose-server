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
package org.safehaus.penrose.server;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;

import javax.naming.directory.SearchResult;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseServiceTest extends TestCase {

    PenroseConfig penroseConfig;
    Penrose penrose;

    public PenroseServiceTest() {
        //PatternLayout patternLayout = new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n");
        PatternLayout patternLayout = new PatternLayout("%-20C{1} [%4L] %m%n");

        ConsoleAppender appender = new ConsoleAppender(patternLayout);
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.INFO);
    }

    public void setUp() throws Exception {

        penroseConfig = new DefaultPenroseConfig();

        SchemaConfig schemaConfig = new SchemaConfig("samples/shop/schema/example.schema");
        penroseConfig.addSchemaConfig(schemaConfig);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

    }

    public void tearDown() throws Exception {
        penrose.stop();
    }

    public void testStartingAndStopping() throws Exception {

        PenroseSession session = penrose.newSession();

        try {
            session.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());
            search(session);
        } catch (Exception e) {
            fail("Bind & search should not fail");
        }

        assertEquals(Penrose.STARTED, penrose.getStatus());

        System.out.println("Stopping Penrose");

        penrose.stop();

        assertEquals(Penrose.STOPPED, penrose.getStatus());

        try {
            session.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());
            fail();
        } catch (Exception e) {
            System.out.println("Bind failed as expected");
        }

        PenroseSession session2 = penrose.newSession();
        assertNull(session2);

        System.out.println("Starting Penrose");

        penrose.start();

        assertEquals(Penrose.STARTED, penrose.getStatus());

        try {
            session.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());
            fail();
        } catch (Exception e) {
            System.out.println("Bind failed as expected");
        }

        try {
            PenroseSession session3 = penrose.newSession();
            session3.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());
            search(session3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Bind & search should not fail");
        }
    }

    public void testPenroseService() throws Exception {
        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());

        search(session);
        session.close();
    }

    public void search(PenroseSession session) throws Exception {

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);

        String baseDn = "ou=Categories,dc=Shop,dc=Example,dc=com";

        System.out.println("Searching "+baseDn+":");
        session.search(baseDn, "(objectClass=*)", sc, results);

        for (Iterator i = results.iterator(); i.hasNext();) {
            SearchResult entry = (SearchResult) i.next();
            System.out.println("dn: "+entry.getName());
        }
    }
}
