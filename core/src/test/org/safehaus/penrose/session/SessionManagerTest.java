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
package org.safehaus.penrose.session;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.user.UserConfig;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

/**
 * @author Endi S. Dewata
 */
public class SessionManagerTest extends TestCase {

    PenroseConfig penroseConfig;
    Penrose penrose;

    public void setUp() throws Exception {

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.INFO);

        penroseConfig = new DefaultPenroseConfig();

        SessionConfig sessionConfig = penroseConfig.getSessionConfig();
        sessionConfig.setParameter(SessionConfig.MAX_SESSIONS, "6");
        sessionConfig.setParameter(SessionConfig.MAX_IDLE_TIME, "1"); // 1 minute

        SchemaConfig schemaConfig = new SchemaConfig("samples/shop/schema/example.schema");
        penroseConfig.addSchemaConfig(schemaConfig);

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

    }

    public void tearDown() throws Exception {
        penrose.stop();
    }

    public void testMaxSessions() throws Exception {

        SessionManager sessionManager = penrose.getSessionManager();
        assertEquals(0, sessionManager.getSessions().size());

        Collection sessions = new ArrayList();

        System.out.println("Creating 6 sessions");

        for (int i=0; i<6; i++) {
            PenroseSession session = penrose.newSession();
            assertNotNull("Session should not be null", session);
            if (session != null) sessions.add(session);
        }

        assertEquals(6, sessionManager.getSessions().size());

        System.out.println("Creating 6 more sessions");

        for (int i=0; i<6; i++) {
            PenroseSession session = penrose.newSession();
            assertNull("Session should be null", session);
        }

        assertEquals(6, sessionManager.getSessions().size());

        System.out.println("Closing 6 sessions");

        for (Iterator i=sessions.iterator(); i.hasNext(); ) {
            PenroseSession session = (PenroseSession)i.next();
            session.close();
        }

        assertEquals(0, sessionManager.getSessions().size());
    }

    public void testMaxIdleTime() throws Exception {

        SessionManager sessionManager = penrose.getSessionManager();
        UserConfig rootUserConfig = penroseConfig.getRootUserConfig();
        String bindDn = rootUserConfig.getDn();
        String password = rootUserConfig.getPassword();

        assertEquals(0, sessionManager.getSessions().size());

        Collection sessions = new ArrayList();

        for (int i=0; i<6; i++) {

            PenroseSession session = penrose.newSession();
            System.out.println("Created 1 session at "+session.getCreateDate());

            assertNotNull("Session should not be null", session);

            session.bind(bindDn, password);

            sessions.add(session);

            assertEquals(i+1, sessionManager.getSessions().size());

            Thread.sleep(10 * 1000); // wait 10 seconds
        }

        System.out.println("Waiting for 10 more seconds...");
        Thread.sleep(10 * 1000); // wait 10 seconds

        int c = 5;
        for (Iterator i=sessions.iterator(); i.hasNext(); c--) {

            PenroseSession session = (PenroseSession)i.next();

            Date date = new Date();
            int size = sessionManager.getSessions().size();
            System.out.println("There are "+size+" remaining session(s) at "+date);

            assertEquals(c, size);

            try {
                session.bind(bindDn, password);
                fail("Bind should fail");

            } catch (Exception e) {
                System.out.println("Bind failed as expected");
            }

            Thread.sleep(10 * 1000); // wait 10 seconds
        }

    }
}
