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
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class BuiltInEntriesTest extends TestCase {

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

        PenroseFactory penroseFactory = PenroseFactory.getInstance();
        penrose = penroseFactory.createPenrose(penroseConfig);
        penrose.start();

    }

    public void tearDown() throws Exception {
        penrose.stop();
    }

    public void testSearch() throws Exception {

        PenroseSession session = penrose.newSession();
        session.bind(penroseConfig.getRootUserConfig().getDn(), penroseConfig.getRootUserConfig().getPassword());

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_ONE);

        String baseDn = "ou=system";

        System.out.println("Searching "+baseDn+":");
        session.search(baseDn, "(objectClass=*)", sc, results);

        assertEquals(0, results.size());
        assertEquals(LDAPException.NO_SUCH_OBJECT, results.getReturnCode());

        session.unbind();

        session.close();
    }
}
