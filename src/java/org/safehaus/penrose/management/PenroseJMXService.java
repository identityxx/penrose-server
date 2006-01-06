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

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.service.Service;
import org.safehaus.penrose.service.ServiceConfig;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXService extends Service {

    public final static String RMI_PORT       = "rmiPort";
    public final static int DEFAULT_RMI_PORT  = 1099;

    public final static String HTTP_PORT      = "httpPort";
    public final static int DEFAULT_HTTP_PORT = 8112;

    PenroseJMXAuthenticator jmxAuthenticator;

    MBeanServer mbeanServer;

    ObjectName penroseAdminName = ObjectName.getInstance(PenroseClient.MBEAN_NAME);
    PenroseAdmin penroseAdmin;

    ObjectName registryName = ObjectName.getInstance("naming:type=rmiregistry");
    NamingService registry;

    ObjectName rmiConnectorName = ObjectName.getInstance("connectors:type=rmi,protocol=jrmp");
    JMXConnectorServer rmiConnector;

    ObjectName httpConnectorName = ObjectName.getInstance("connectors:type=http");
    HttpAdaptor httpConnector;

    ObjectName xsltProcessorName = ObjectName.getInstance("connectors:type=http,processor=xslt");
    XSLTProcessor xsltProcessor;

    private int rmiPort;
    private int httpPort;

    static {
        System.setProperty("jmx.invoke.getters", "true");
        System.setProperty("javax.management.builder.initial", "mx4j.server.MX4JMBeanServerBuilder");
    }

    public PenroseJMXService() throws Exception {
        mx4j.log.Log.redirectTo(new Log4JLogger());
    }

    public void init() throws Exception {
        ServiceConfig serviceConfig = getServiceConfig();

        String s = serviceConfig.getParameter(RMI_PORT);
        rmiPort = s == null ? DEFAULT_RMI_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(HTTP_PORT);
        httpPort = s == null ? DEFAULT_HTTP_PORT : Integer.parseInt(s);
    }

    public void start() throws Exception {

        log.warn("Starting JMX Service.");

        setStatus(STARTING);

        mbeanServer = MBeanServerFactory.createMBeanServer();

        penroseAdmin = new PenroseAdmin();
        penroseAdmin.setPenroseServer(getPenroseServer());

        mbeanServer.registerMBean(penroseAdmin, penroseAdminName);

        if (rmiPort > 0) {

            registry = new NamingService(rmiPort);
            mbeanServer.registerMBean(registry, registryName);
            registry.start();

            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:"+rmiPort+"/jmx");
            jmxAuthenticator = new PenroseJMXAuthenticator(getPenroseServer().getPenrose());

            HashMap environment = new HashMap();
            environment.put("jmx.remote.authenticator", jmxAuthenticator);

            rmiConnector = JMXConnectorServerFactory.newJMXConnectorServer(url, environment, null);
            mbeanServer.registerMBean(rmiConnector, rmiConnectorName);
            rmiConnector.start();

            log.warn("Listening to port "+rmiPort+".");
        }

        if (httpPort > 0) {
            xsltProcessor = new XSLTProcessor();
            mbeanServer.registerMBean(xsltProcessor, xsltProcessorName);

            httpConnector = new HttpAdaptor(8112, "localhost");
            httpConnector.setProcessorName(xsltProcessorName);
            mbeanServer.registerMBean(httpConnector, httpConnectorName);
            httpConnector.start();

            log.warn("Listening to port "+httpPort+".");
        }

        setStatus(STARTED);
    }

    public void stop() throws Exception {

        if (httpPort > 0) {
            httpConnector.stop();
            mbeanServer.unregisterMBean(httpConnectorName);
            mbeanServer.unregisterMBean(xsltProcessorName);
        }

        if (rmiPort > 0) {
            rmiConnector.stop();
            mbeanServer.unregisterMBean(rmiConnectorName);

            registry.stop();
            mbeanServer.unregisterMBean(registryName);
        }

        mbeanServer.unregisterMBean(penroseAdminName);
        MBeanServerFactory.releaseMBeanServer(mbeanServer);

        setStatus(STOPPED);

        log.warn("JMX Service has been shutdown.");
    }

    public int getRmiPort() {
        return rmiPort;
    }

    public void setRmiPort(int rmiPort) {
        this.rmiPort = rmiPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }
}
