package org.safehaus.penrose.test.quick.session;

import junit.framework.TestCase;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.session.SessionConfig;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.PenroseSession;
import org.apache.log4j.*;

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

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.INFO);
        logger.setAdditivity(false);

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

        for (int i=0; i<6; i++) {
            PenroseSession session = penrose.newSession();
            assertNotNull("Session should not be null", session);
            if (session != null) sessions.add(session);
        }

        assertEquals(6, sessionManager.getSessions().size());

        for (int i=0; i<6; i++) {
            PenroseSession session = penrose.newSession();
            assertNull("Session should be null", session);
        }

        assertEquals(6, sessionManager.getSessions().size());

        for (Iterator i=sessions.iterator(); i.hasNext(); ) {
            PenroseSession session = (PenroseSession)i.next();
            session.close();
        }

        assertEquals(0, sessionManager.getSessions().size());
    }
}
