/**
 * Copyright 2009 Red Hat, Inc.
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

import org.safehaus.penrose.service.Service;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.client.PenroseClient;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXService extends Service {

    public final static String RMI_PORT           = "rmiPort";
    public final static String RMI_TRANSPORT_PORT = "rmiTransportPort";

    protected int rmiPort;
    protected int rmiTransportPort;

    protected MBeanServer mbeanServer;
    protected PenroseConnectorServer connectorServer;

    protected PenroseAuthenticator authenticator;
    protected PenroseService penroseService;

    static {
        //System.setProperty("jmx.invoke.getters", "true");
    }

    public PenroseJMXService() throws Exception {
    }

    public void init() throws Exception {
        super.init();

        ServiceConfig serviceConfig = getServiceConfig();

        String s = serviceConfig.getParameter(RMI_PORT);
        rmiPort = s == null ? PenroseClient.DEFAULT_RMI_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(RMI_TRANSPORT_PORT);
        rmiTransportPort = s == null ? PenroseClient.DEFAULT_RMI_TRANSPORT_PORT : Integer.parseInt(s);

        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        
        penroseService = new PenroseService(this, serviceContext.getPenroseServer());
        penroseService.init();

        if (rmiPort > 0) {

            LocateRegistry.createRegistry(rmiPort);
            
            String url = "service:jmx:rmi://localhost";
            if (rmiTransportPort != PenroseClient.DEFAULT_RMI_TRANSPORT_PORT) url += ":"+rmiTransportPort;

            url += "/jndi/rmi://localhost";
            if (rmiPort != PenroseClient.DEFAULT_RMI_PORT) url += ":"+rmiPort;

            url += "/penrose";
            //String url = "service:jmx:rmi://localhost:rmiTransportProtocol/jndi:rmi://localhost:rmiProtocol/penrose";

            JMXServiceURL serviceURL = new JMXServiceURL(url);
            authenticator = new PenroseAuthenticator(serviceContext.getPenroseServer().getPenrose());

            HashMap<String,Object> environment = new HashMap<String,Object>();
            environment.put(JMXConnectorServer.AUTHENTICATOR, authenticator);

            connectorServer = new PenroseConnectorServer(serviceURL, environment, mbeanServer);
            connectorServer.start();

            log.warn("Listening to port "+rmiPort+" (RMI).");
            if (rmiTransportPort != PenroseClient.DEFAULT_RMI_TRANSPORT_PORT) log.warn("Listening to port "+rmiTransportPort+" (RMI Transport).");
        }
    }

    public void destroy() throws Exception {

        if (rmiPort > 0) {
            connectorServer.stop();
        }

        penroseService.destroy();

        log.warn("JMX Service has been shutdown.");
    }

    public int getRmiPort() {
        return rmiPort;
    }

    public void setRmiPort(int rmiPort) {
        this.rmiPort = rmiPort;
    }

    public void register(String name, Object object) throws Exception {
        register(ObjectName.getInstance(name), object);
    }

    public void register(ObjectName objectName, Object object) throws Exception {
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Registering "+objectName);
        if (mbeanServer.isRegistered(objectName)) mbeanServer.unregisterMBean(objectName);
        mbeanServer.registerMBean(object, objectName);
    }

    public void unregister(String name) throws Exception {
        unregister(ObjectName.getInstance(name));
    }

    public void unregister(ObjectName objectName) throws Exception {
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Unregistering "+objectName);
        if (mbeanServer.isRegistered(objectName)) mbeanServer.unregisterMBean(objectName);
    }

    public int getRmiTransportPort() {
        return rmiTransportPort;
    }

    public void setRmiTransportPort(int rmiTransportPort) {
        this.rmiTransportPort = rmiTransportPort;
    }

    public MBeanServer getMBeanServer() {
        return mbeanServer;
    }

    public void setMBeanServer(MBeanServer mbeanServer) {
        this.mbeanServer = mbeanServer;
    }

    public JMXConnectorServer getConnectorServer() {
        return connectorServer;
    }

    public void setConnectorServer(PenroseConnectorServer connectorServer) {
        this.connectorServer = connectorServer;
    }

    public PenroseAuthenticator getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(PenroseAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    public PenroseService getPenroseService() {
        return penroseService;
    }

    public void setPenroseService(PenroseService penroseService) {
        this.penroseService = penroseService;
    }
}
