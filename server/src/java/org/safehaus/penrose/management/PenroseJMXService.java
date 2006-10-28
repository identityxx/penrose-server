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
package org.safehaus.penrose.management;

import mx4j.tools.naming.NamingService;
//import mx4j.tools.adaptor.http.HttpAdaptor;
//import mx4j.tools.adaptor.http.XSLTProcessor;
import mx4j.log.Log4JLogger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.MBeanServerFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.JMXConnectorServerFactory;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;

import org.safehaus.penrose.service.Service;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.service.ServiceManager;
import org.safehaus.penrose.service.ServiceManagerMBean;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.ConnectionManagerMBean;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.module.ModuleManagerMBean;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.config.PenroseConfig;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXService extends Service {

    public final static String RMI_PORT           = "rmiPort";
    public final static String RMI_TRANSPORT_PORT = "rmiTransportPort";
    public final static String HTTP_PORT          = "httpPort";

    public final static int DEFAULT_RMI_PORT            = 1099;
    public final static int DEFAULT_RMI_TRANSPORT_PORT  = 0;
    public final static int DEFAULT_HTTP_PORT           = 8112;
    public final static String DEFAULT_PROTOCOL         = "rmi";

    PenroseJMXAuthenticator jmxAuthenticator;

    MBeanServer mbeanServer;
    boolean createdMBeanServer;

    ObjectName penroseServiceName = ObjectName.getInstance(PenroseServiceMBean.NAME);
    PenroseService penroseService;

    ObjectName registryName = ObjectName.getInstance("naming:type=rmiregistry");
    NamingService registry;

    ObjectName rmiConnectorName = ObjectName.getInstance("connectors:type=rmi,protocol=jrmp");
    JMXConnectorServer rmiConnector;

    ObjectName httpConnectorName = ObjectName.getInstance("connectors:type=http");
    //HttpAdaptor httpConnector;
    JMXConnectorServer httpConnector;

    //ObjectName xsltProcessorName = ObjectName.getInstance("connectors:type=http,processor=xslt");
    //XSLTProcessor xsltProcessor;

    private int rmiPort;
    private int rmiTransportPort;
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

        s = serviceConfig.getParameter(RMI_TRANSPORT_PORT);
        rmiTransportPort = s == null ? DEFAULT_RMI_TRANSPORT_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(HTTP_PORT);
        httpPort = s == null ? DEFAULT_HTTP_PORT : Integer.parseInt(s);
    }

    public void start() throws Exception {

        //log.warn("Starting JMX Service.");

        setStatus(STARTING);

        Collection servers = MBeanServerFactory.findMBeanServer(null);
        if (servers.isEmpty()) {
            mbeanServer = MBeanServerFactory.createMBeanServer();
            createdMBeanServer = true;
        } else {
            mbeanServer = (MBeanServer)servers.iterator().next();
        }

        if (!mbeanServer.isRegistered(penroseServiceName)) {
            penroseService = new PenroseService(getPenroseServer());
            mbeanServer.registerMBean(penroseService, penroseServiceName);
        }

        register();

        jmxAuthenticator = new PenroseJMXAuthenticator(getPenroseServer().getPenrose());

        HashMap environment = new HashMap();
        environment.put("jmx.remote.authenticator", jmxAuthenticator);

        if (rmiPort > 0 && createdMBeanServer) {
            registry = new NamingService(rmiPort);
            mbeanServer.registerMBean(registry, registryName);
            registry.start();

            String url = "service:jmx:rmi://localhost";
            if (rmiTransportPort != DEFAULT_RMI_TRANSPORT_PORT) url += ":"+rmiTransportPort;

            url += "/jndi/rmi://localhost";
            //if (rmiPort != PenroseClient.DEFAULT_RMI_PORT)
            url += ":"+rmiPort;

            url += "/jmx";

            JMXServiceURL serviceURL = new JMXServiceURL(url);
            //JMXServiceURL serviceURL = new JMXServiceURL("rmi", "localhost", rmiPort, null);
            log.debug("Running RMI Connector at "+serviceURL);

            rmiConnector = JMXConnectorServerFactory.newJMXConnectorServer(
                    serviceURL,
                    environment,
                    mbeanServer
            );
            mbeanServer.registerMBean(rmiConnector, rmiConnectorName);
            rmiConnector.start();

            log.warn("Listening to port "+rmiPort+" (RMI).");
            if (rmiTransportPort != DEFAULT_RMI_TRANSPORT_PORT) log.warn("Listening to port "+rmiTransportPort+" (RMI Transport).");
        }

        if (httpPort > 0) {
            JMXServiceURL serviceURL = new JMXServiceURL("soap", null, httpPort, "/jmxconnector");
            log.debug("Running HTTP Connector at "+serviceURL);

            httpConnector = JMXConnectorServerFactory.newJMXConnectorServer(
                    serviceURL,
                    null,
                    mbeanServer
            );
            mbeanServer.registerMBean(httpConnector, httpConnectorName);
            httpConnector.start();
/*
            xsltProcessor = new XSLTProcessor();
            mbeanServer.registerMBean(xsltProcessor, xsltProcessorName);

            httpConnector = new HttpAdaptor(httpPort, "localhost");
            httpConnector.setProcessorName(xsltProcessorName);
            mbeanServer.registerMBean(httpConnector, httpConnectorName);
            httpConnector.start();
*/
            log.warn("Listening to port "+httpPort+" (HTTP).");
        }

        setStatus(STARTED);
    }

    public void stop() throws Exception {

        if (httpPort > 0) {
            httpConnector.stop();
            mbeanServer.unregisterMBean(httpConnectorName);
            //mbeanServer.unregisterMBean(xsltProcessorName);
        }

        if (rmiPort > 0 && createdMBeanServer) {
            rmiConnector.stop();
            mbeanServer.unregisterMBean(rmiConnectorName);

            registry.stop();
            mbeanServer.unregisterMBean(registryName);
        }

        unregister();

        if (penroseService != null) {
            mbeanServer.unregisterMBean(penroseServiceName);
        }

        if (createdMBeanServer) MBeanServerFactory.releaseMBeanServer(mbeanServer);

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

    public void register(String name, Object object) throws Exception {
        log.debug("Registering "+name);
        ObjectName on = ObjectName.getInstance(name);
        if (mbeanServer.isRegistered(on)) mbeanServer.unregisterMBean(on);
        mbeanServer.registerMBean(object, on);
    }

    public void unregister(String name) throws Exception {
        log.debug("Unregistering "+name);
        ObjectName on = ObjectName.getInstance(name);
        if (mbeanServer.isRegistered(on)) mbeanServer.unregisterMBean(on);
    }

    public void register() throws Exception {
        registerConfigs();
        //registerManagers();
        registerConnections();
        registerSources();
        registerModules();
        registerPartitions();
        registerServices();
    }

    public void unregister() throws Exception {
        unregisterServices();
        unregisterPartitions();
        unregisterModules();
        unregisterSources();
        unregisterConnections();
        //unregisterManagers();
        unregisterConfigs();
    }

    public void registerServices() throws Exception {
        PenroseServer penroseServer = getPenroseServer();
        ServiceManager serviceManager = penroseServer.getServiceManager();

        register(ServiceManagerMBean.NAME, serviceManager);

        for (Iterator i=serviceManager.getServiceNames().iterator(); i.hasNext(); ) {
            String serviceName = (String)i.next();
            Service service = serviceManager.getService(serviceName);
            register("Penrose Services:name="+serviceName+",type=Service", service);
        }
    }

    public void unregisterServices() throws Exception {
        PenroseServer penroseServer = getPenroseServer();
        ServiceManager serviceManager = penroseServer.getServiceManager();

        for (Iterator i=serviceManager.getServiceNames().iterator(); i.hasNext(); ) {
            String serviceName = (String)i.next();
            unregister("Penrose Services:name="+serviceName+",type=Service");
        }

        unregister(ServiceManagerMBean.NAME);
    }

    public void registerConfigs() throws Exception {

        Penrose penrose = getPenroseServer().getPenrose();
        PenroseConfig penroseConfig = penrose.getPenroseConfig();

        register("Penrose Config:type=PenroseConfig", penroseConfig);
        register("Penrose Config:type=SessionConfig", penroseConfig.getSessionConfig());

        Collection schemaConfigs = penroseConfig.getSchemaConfigs();
        for (Iterator i=schemaConfigs.iterator(); i.hasNext(); ) {
            SchemaConfig schemaConfig = (SchemaConfig)i.next();
            register("Penrose Config:name="+schemaConfig.getName()+",type=SchemaConfig", schemaConfig);
        }

        Collection adapterConfigs = penroseConfig.getAdapterConfigs();
        for (Iterator i=adapterConfigs.iterator(); i.hasNext(); ) {
            AdapterConfig adapterConfig = (AdapterConfig)i.next();
            register("Penrose Config:name="+adapterConfig.getName()+",type=AdapterConfig", adapterConfig);
        }

        Collection partitionConfigs = penroseConfig.getPartitionConfigs();
        for (Iterator i=partitionConfigs.iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            register("Penrose Config:name="+partitionConfig.getName()+",type=PartitionConfig", partitionConfig);
        }

        Collection serviceConfigs = penroseConfig.getServiceConfigs();
        for (Iterator i=serviceConfigs.iterator(); i.hasNext(); ) {
            ServiceConfig serviceConfig = (ServiceConfig)i.next();
            register("Penrose Config:name="+serviceConfig.getName()+",type=ServiceConfig", serviceConfig);
        }
    }

    public void unregisterConfigs() throws Exception {

        Penrose penrose = getPenroseServer().getPenrose();
        PenroseConfig penroseConfig = penrose.getPenroseConfig();

        Collection serviceConfigs = penroseConfig.getServiceConfigs();
        for (Iterator i=serviceConfigs.iterator(); i.hasNext(); ) {
            ServiceConfig serviceConfig = (ServiceConfig)i.next();
            unregister("Penrose Config:name="+serviceConfig.getName()+",type=ServiceConfig");
        }

        Collection partitionConfigs = penroseConfig.getPartitionConfigs();
        for (Iterator i=partitionConfigs.iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            unregister("Penrose Config:name="+partitionConfig.getName()+",type=PartitionConfig");
        }

        Collection adapterConfigs = penroseConfig.getAdapterConfigs();
        for (Iterator i=adapterConfigs.iterator(); i.hasNext(); ) {
            AdapterConfig adapterConfig = (AdapterConfig)i.next();
            unregister("Penrose Config:name="+adapterConfig.getName()+",type=AdapterConfig");
        }

        Collection schemaConfigs = penroseConfig.getSchemaConfigs();
        for (Iterator i=schemaConfigs.iterator(); i.hasNext(); ) {
            SchemaConfig schemaConfig = (SchemaConfig)i.next();
            unregister("Penrose Config:name="+schemaConfig.getName()+",type=SchemaConfig");
        }

        unregister("Penrose Config:type=SessionConfig");
        unregister("Penrose Config:type=PenroseConfig");
    }

    public void registerManagers() throws Exception {
        Penrose penrose = getPenroseServer().getPenrose();

        register("Penrose:name=SchemaManager", penrose.getSchemaManager());
        register("Penrose:name=ConnectionManager", penrose.getConnectionManager());
        register("Penrose:name=PartitionManager", penrose.getPartitionManager());
        register("Penrose:name=ModuleManager", penrose.getModuleManager());
        register("Penrose:name=SessionManager", penrose.getSessionManager());
        register("Penrose:name=ServiceManager", getPenroseServer().getServiceManager());
    }

    public void unregisterManagers() throws Exception {
        unregister("Penrose:name=ServiceManager");
        unregister("Penrose:name=SessionManager");
        unregister("Penrose:name=ModuleManager");
        unregister("Penrose:name=PartitionManager");
        unregister("Penrose:name=ConnectionManager");
        unregister("Penrose:name=SchemaManager");
    }

    public void registerConnections() throws Exception {
        Penrose penrose = getPenroseServer().getPenrose();
        ConnectionManager connectionManager = penrose.getConnectionManager();

        register(ConnectionManagerMBean.NAME, connectionManager);

        for (Iterator i=connectionManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=connectionManager.getConnectionNames(partitionName).iterator(); j.hasNext(); ) {
                String connectionName = (String)j.next();
                Connection connection = connectionManager.getConnection(partitionName, connectionName);

                register("Penrose Connections:name="+connectionName+",partition="+partitionName+",type=Connection", connection);
            }
        }
    }

    public void unregisterConnections() throws Exception {

        Penrose penrose = getPenroseServer().getPenrose();
        ConnectionManager connectionManager = penrose.getConnectionManager();

        register(ConnectionManagerMBean.NAME, connectionManager);

        for (Iterator i=connectionManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=connectionManager.getConnectionNames(partitionName).iterator(); j.hasNext(); ) {
                String connectionName = (String)j.next();
                unregister("Penrose Connections:name="+connectionName+",partition="+partitionName+",type=Connection");
            }
        }

        unregister(ConnectionManagerMBean.NAME);
    }

    public void registerSources() throws Exception {
        Penrose penrose = getPenroseServer().getPenrose();
        SourceManager sourceManager = penrose.getSourceManager();

        register(SourceManagerMBean.NAME, sourceManager);

        for (Iterator i=sourceManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=sourceManager.getSourceNames(partitionName).iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                Source source = sourceManager.getSource(partitionName, sourceName);

                register("Penrose Sources:name="+sourceName +",partition="+partitionName+",type=Source", source);
            }
        }
    }

    public void unregisterSources() throws Exception {
        Penrose penrose = getPenroseServer().getPenrose();
        SourceManager sourceManager = penrose.getSourceManager();

        for (Iterator i=sourceManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=sourceManager.getSourceNames(partitionName).iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                unregister("Penrose Sources:name="+sourceName +",partition="+partitionName+",type=Source");
            }
        }

        unregister(SourceManagerMBean.NAME);
    }

    public void registerModules() throws Exception {
        Penrose penrose = getPenroseServer().getPenrose();
        ModuleManager moduleManager = penrose.getModuleManager();

        register(ModuleManagerMBean.NAME, moduleManager);

        for (Iterator i=moduleManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=moduleManager.getModuleNames(partitionName).iterator(); j.hasNext(); ) {
                String moduleName = (String)j.next();
                Module module = moduleManager.getModule(partitionName, moduleName);

                register("Penrose Modules:name="+moduleName +",partition="+partitionName+",type=Module", module);
            }
        }
    }

    public void unregisterModules() throws Exception {
        Penrose penrose = getPenroseServer().getPenrose();
        ModuleManager moduleManager = penrose.getModuleManager();

        for (Iterator i=moduleManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();

            for (Iterator j=moduleManager.getModuleNames(partitionName).iterator(); j.hasNext(); ) {
                String moduleName = (String)j.next();
                unregister("Penrose Modules:name="+moduleName +",partition="+partitionName+",type=Module");
            }
        }

        unregister(ModuleManagerMBean.NAME);
    }

    public void registerPartitions() throws Exception {

        Penrose penrose = getPenroseServer().getPenrose();
        PartitionManager partitionManager = penrose.getPartitionManager();

        register(PartitionManagerMBean.NAME, partitionManager);

        for (Iterator i=partitionManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Partition partition = partitionManager.getPartition(name);

            register("Penrose Partitions:name="+name+",type=Partition", partition);

            registerConnections(partition);
            //registerSources(partition);
            registerModules(partition);
        }
    }

    public void unregisterPartitions() throws Exception {

        Penrose penrose = getPenroseServer().getPenrose();
        PartitionManager partitionManager = penrose.getPartitionManager();

        for (Iterator i=partitionManager.getPartitionNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Partition partition = partitionManager.getPartition(name);

            unregisterModules(partition);
            //unregisterSources(partition);
            unregisterConnections(partition);

            unregister("Penrose Partitions:name="+name+",type=Partition");
        }

        unregister(PartitionManagerMBean.NAME);
    }

    public void registerConnections(Partition partition) throws Exception {

        Collection connectionConfigs = partition.getConnectionConfigs();
        for (Iterator i=connectionConfigs.iterator(); i.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();

            String name = "Penrose Config:name="+connectionConfig.getName()+",partition="+partition.getName()+",type=ConnectionConfig";
            register(name, connectionConfig);
        }
    }

    public void unregisterConnections(Partition partition) throws Exception {

        Collection connectionConfigs = partition.getConnectionConfigs();
        for (Iterator i=connectionConfigs.iterator(); i.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();

            String name = "Penrose Config:name="+connectionConfig.getName()+",partition="+partition.getName()+",type=ConnectionConfig";
            unregister(name);
        }
    }

    public void registerSources(Partition partition) throws Exception {

        Collection sourceConfigs = partition.getSourceConfigs();
        for (Iterator i=sourceConfigs.iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();

            String name = "Penrose Config:name="+sourceConfig.getName()+",partition="+partition.getName()+",type=SourceConfig";
            register(name, sourceConfig);

            registerFields(partition, sourceConfig);
        }
    }

    public void unregisterSources(Partition partition) throws Exception {

        Collection sourceConfigs = partition.getSourceConfigs();
        for (Iterator i=sourceConfigs.iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();

            unregisterFields(partition, sourceConfig);

            String name = "Penrose Config:name="+sourceConfig.getName()+",partition="+partition.getName()+",type=SourceConfig";
            unregister(name);
        }
    }

    public void registerFields(Partition partition, SourceConfig sourceConfig) throws Exception {

        Collection fieldConfigs = sourceConfig.getFieldConfigs();
        for (Iterator i=fieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = "Penrose Config:name="+fieldConfig.getName()+",source="+sourceConfig.getName()+",partition="+partition.getName()+",type=FieldConfig";
            register(name, fieldConfig);
        }
    }

    public void unregisterFields(Partition partition, SourceConfig sourceConfig) throws Exception {

        Collection fieldConfigs = sourceConfig.getFieldConfigs();
        for (Iterator i=fieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            String name = "Penrose Config:name="+fieldConfig.getName()+",source="+sourceConfig.getName()+",partition="+partition.getName()+",type=FieldConfig";
            unregister(name);
        }
    }

    public void registerModules(Partition partition) throws Exception {

        Collection moduleConfigs = partition.getModuleConfigs();
        for (Iterator i=moduleConfigs.iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();

            String name = "Penrose Config:name="+moduleConfig.getName()+",partition="+partition.getName()+",type=ModuleConfig";
            register(name, moduleConfig);
        }
    }

    public void unregisterModules(Partition partition) throws Exception {

        Collection moduleConfigs = partition.getModuleConfigs();
        for (Iterator i=moduleConfigs.iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();

            String name = "Penrose Config:name="+moduleConfig.getName()+",partition="+partition.getName()+",type=ModuleConfig";
            unregister(name);
        }
    }
}
