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
package org.safehaus.penrose.test.session;

import junit.framework.TestCase;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.session.SessionConfig;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionContext;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SessionManagerTest extends TestCase {

    PenroseConfig penroseConfig;
    Penrose penrose;

    public void setUp() throws Exception {
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

        SessionContext sessionContext = penrose.getSessionContext();
        SessionManager sessionManager = sessionContext.getSessionManager();

        Collection sessions = new ArrayList();

        for (int i=0; i<6; i++) {
            Session session = penrose.createSession();
            assertNotNull("Session should not be null", session);
            if (session != null) sessions.add(session);
        }

        for (int i=0; i<6; i++) {
            Session session = penrose.createSession();
            assertNull("Session should be null", session);
        }

        for (Iterator i=sessions.iterator(); i.hasNext(); ) {
            Session session = (Session)i.next();
            session.close();
        }

    }
/*
    public void testMaxIdleTime() throws Exception {

        SessionManager sessionManager = penrose.getSessionManager();
        UserConfig rootUserConfig = penroseConfig.getRootUserConfig();
        DN bindDn = rootUserConfig.getDn();
        String password = rootUserConfig.getPassword();

        assertEquals(0, sessionManager.getSessions().size());

        Collection sessions = new ArrayList();

        for (int i=0; i<6; i++) {

            Session session = penrose.createSession();
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

            Session session = (Session)i.next();

            Date date = new Date();
            int size = sessionManager.getSessions().size();
            System.out.println("There are "+size+" remaining session(s) at "+date);

            assertEquals(c, size);
            assertFalse(session.isValid());

            Thread.sleep(10 * 1000); // wait 10 seconds
        }

    }
*/
}
