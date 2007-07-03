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
package org.safehaus.penrose.test.management;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.service.ServiceManager;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.management.PenroseJMXService;

/**
 * @author Endi S. Dewata
 */
public class JMXServiceTest extends TestCase {

    PenroseConfig penroseConfig;
    PenroseServer penroseServer;

    public void setUp() throws Exception {

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.INFO);

        penroseConfig = new DefaultPenroseConfig();
        penroseConfig.removeServiceConfig("LDAP");

        penroseServer = new PenroseServer(penroseConfig);
        penroseServer.start();
    }

    public void tearDown() throws Exception {
        penroseServer.stop();
    }

    public void testJMXService() throws Exception {
        ServiceManager serviceManager = penroseServer.getServiceManager();
        PenroseJMXService service = (PenroseJMXService)serviceManager.getService("JMX");
        int port = service.getRmiPort();
        connect(port);
    }

    public void testChangingJMXPort() throws Exception {

        ServiceManager serviceManager = penroseServer.getServiceManager();
        PenroseJMXService service = (PenroseJMXService)serviceManager.getService("JMX");
        int port = service.getRmiPort();

        // testing the old port
        try {
            connect(port);
            System.out.println("Connecting at the old port succeeded as expected.");
        } catch (Exception e) {
            fail("Connecting at the old port should have succeeded.");
        }

        // switching to the new port
        penroseServer.stop();

        service.setRmiPort(port+1);

        penroseServer.start();

        try {
            connect(port);
            fail("Connecting at the old port should have failed.");
        } catch (Exception e) {
            System.out.println("Connecting at the old port failed as expected.");
        }

        try {
            connect(port+1);
            System.out.println("Connecting at the new port succeeded as expected.");
        } catch (Exception e) {
            fail("Connecting at the new port should have succeeded.");
        }

        // switching back to the old port
        penroseServer.stop();

        service.setRmiPort(port);

        penroseServer.start();

        try {
            connect(port);
            System.out.println("Connecting at the old port succeeded.");
        } catch (Exception e) {
            fail("Connecting at the old port should have succeeded as expected.");
        }

        try {
            connect(port+1);
            fail("Connecting at the new port should have failed.");
        } catch (Exception e) {
            System.out.println("Connecting at the old port failed as expected.");
        }
    }

    public void connect(int port) throws Exception {
        System.out.println("Connecting to port "+port);


        PenroseClient client = new PenroseClient("localhost", port, "admin", "secret");
        client.connect();
        String name = client.getProductName();
        String version = client.getProductVersion();
        client.close();

        System.out.println("Connected to "+name+" "+version);
    }
}
