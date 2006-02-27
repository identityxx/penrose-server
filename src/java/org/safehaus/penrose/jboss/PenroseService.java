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
package org.safehaus.penrose.jboss;

import org.apache.log4j.Logger;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.config.PenroseConfig;

//import org.safehaus.penrose.PenroseServer;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    Logger log = Logger.getLogger(getClass());

    private String home;
    PenroseServer server;

    public void create() throws Exception {
        log.info("Creating Penrose Service from "+home);

        server = new PenroseServer(home);

        PenroseConfig config = server.getPenroseConfig();
        ServiceConfig jmxService = config.getServiceConfig("JMX");
        jmxService.setEnabled(false);
/*
        MutableServerStartupConfiguration configuration = new MutableServerStartupConfiguration();
        configuration.setLdapPort(10389);
        configuration.setLdapsPort(10639);

        Set extendedOperationHandlers = new HashSet();
        extendedOperationHandlers.add(new GracefulShutdownHandler());
        extendedOperationHandlers.add(new LaunchDiagnosticUiHandler());
        configuration.setExtendedOperationHandlers(extendedOperationHandlers);

        configuration.setAllowAnonymousAccess(true);

        Properties env = new Properties();
        env.setProperty(Context.PROVIDER_URL, "ou=system");
        env.setProperty(Context.INITIAL_CONTEXT_FACTORY, ServerContextFactory.class.getName() );
        env.setProperty(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.setProperty(Context.SECURITY_CREDENTIALS, "secret");
        env.setProperty(Context.SECURITY_AUTHENTICATION, "simple");

        env.putAll(configuration.toJndiEnvironment());

        new InitialDirContext(env);
*/
        log.info("Penrose Service created.");
    }

    public void start() throws Exception {
        log.info("Starting Penrose Service...");

        server.start();

        log.info("Penrose Service started.");
    }

    public void stop() {
        log.info("Stopping Penrose Service");
    }

    public void destroy() {
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }
}
