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
package org.safehaus.penrose.management;

import mx4j.tools.naming.NamingService;
import mx4j.tools.adaptor.http.HttpAdaptor;
import mx4j.tools.adaptor.http.XSLTProcessor;
import mx4j.log.Log4JLogger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.MBeanServerFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnectorServerFactory;
import java.util.HashMap;

import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.Penrose;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXService {

    public Logger log = Logger.getLogger(PenroseJMXService.class);

    Penrose penrose;
    PenroseAdmin penroseAdmin;

    PenroseJMXAuthenticator jmxAuthenticator;

    MBeanServer mbeanServer;

    ObjectName registryName = ObjectName.getInstance("naming:type=rmiregistry");
    NamingService registry;

    ObjectName rmiConnectorName = ObjectName.getInstance("connectors:type=rmi,protocol=jrmp");
    JMXConnectorServer rmiConnector;

    ObjectName httpConnectorName = ObjectName.getInstance("connectors:type=http");
    HttpAdaptor httpConnector;

    ObjectName xsltProcessorName = ObjectName.getInstance("connectors:type=http,processor=xslt");
    XSLTProcessor xsltProcessor;

    public PenroseJMXService() throws Exception {
        mx4j.log.Log.redirectTo(new Log4JLogger());
    }

    public void start() throws Exception {

        ServerConfig serverConfig = penrose.getServerConfig();

        mbeanServer = MBeanServerFactory.createMBeanServer();

        penroseAdmin = new PenroseAdmin();
        penroseAdmin.setPenrose(penrose);

        mbeanServer.registerMBean(penroseAdmin, ObjectName.getInstance(PenroseClient.MBEAN_NAME));

        registry = new NamingService(serverConfig.getJmxRmiPort());
        mbeanServer.registerMBean(registry, registryName);
        registry.start();

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:"+serverConfig.getJmxRmiPort()+"/jmx");
        jmxAuthenticator = new PenroseJMXAuthenticator("ldap://localhost:"+serverConfig.getPort(), "uid={0},ou=system");

        HashMap environment = new HashMap();
        environment.put("jmx.remote.authenticator", jmxAuthenticator);

        rmiConnector = JMXConnectorServerFactory.newJMXConnectorServer(url, environment, null);
        mbeanServer.registerMBean(rmiConnector, rmiConnectorName);
        rmiConnector.start();

        log.warn("Listening to port "+serverConfig.getJmxRmiPort()+".");

        xsltProcessor = new XSLTProcessor();
        mbeanServer.registerMBean(xsltProcessor, xsltProcessorName);

        httpConnector = new HttpAdaptor(8112, "localhost");
        httpConnector.setProcessorName(xsltProcessorName);
        mbeanServer.registerMBean(httpConnector, httpConnectorName);
        httpConnector.start();

        log.warn("Listening to port "+serverConfig.getJmxHttpPort()+".");
    }

    public void stop() throws Exception {
        httpConnector.stop();
        rmiConnector.stop();
        registry.stop();

        mbeanServer.unregisterMBean(httpConnectorName);
        mbeanServer.unregisterMBean(xsltProcessorName);
        mbeanServer.unregisterMBean(rmiConnectorName);
        mbeanServer.unregisterMBean(registryName);
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }
}
