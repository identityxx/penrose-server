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
import java.util.Collection;
import java.lang.management.ManagementFactory;

import org.safehaus.penrose.service.Service;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.session.SessionContext;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.config.PenroseConfig;

/**
 * @author Endi S. Dewata
 */
public class PenroseJMXService extends Service {

    public final static String RMI_PORT           = "rmiPort";
    public final static String RMI_TRANSPORT_PORT = "rmiTransportPort";
    public final static String HTTP_PORT          = "httpPort";

    PenroseJMXAuthenticator jmxAuthenticator;

    MBeanServer mbeanServer;
    boolean createdMBeanServer;

    PenroseService penroseService;

    ObjectName registryName = ObjectName.getInstance("naming:type=rmiregistry");
    NamingService registry;

    ObjectName rmiConnectorName = ObjectName.getInstance("connectors:type=rmi,protocol=jrmp");
    JMXConnectorServer rmiConnector;

    ObjectName httpConnectorName = ObjectName.getInstance("connectors:type=http");
    HttpAdaptor httpConnector;

    ObjectName xsltProcessorName = ObjectName.getInstance("connectors:type=http,processor=xslt");
    XSLTProcessor xsltProcessor;

    private int rmiPort;
    private int rmiTransportPort;
    private int httpPort;

    static {
        System.setProperty("jmx.invoke.getters", "true");
    }

    public PenroseJMXService() throws Exception {
        mx4j.log.Log.redirectTo(new Log4JLogger());
    }

    public void init() throws Exception {
        ServiceConfig serviceConfig = getServiceConfig();

        String s = serviceConfig.getParameter(RMI_PORT);
        rmiPort = s == null ? PenroseClient.DEFAULT_RMI_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(RMI_TRANSPORT_PORT);
        rmiTransportPort = s == null ? PenroseClient.DEFAULT_RMI_TRANSPORT_PORT : Integer.parseInt(s);

        s = serviceConfig.getParameter(HTTP_PORT);
        httpPort = s == null ? PenroseClient.DEFAULT_HTTP_PORT : Integer.parseInt(s);
    }

    public void start() throws Exception {

        //log.warn("Starting JMX Service.");

        setStatus(STARTING);
/*
        Collection servers = MBeanServerFactory.findMBeanServer(null);
        if (servers.isEmpty()) {
            mbeanServer = MBeanServerFactory.createMBeanServer();
            createdMBeanServer = true;
        } else {
            mbeanServer = (MBeanServer)servers.iterator().next();
        }
*/
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        penroseService = new PenroseService(this, serviceContext.getPenroseServer());

        register();

        if (rmiPort > 0) { // && createdMBeanServer) {

            registry = new NamingService(rmiPort);
            mbeanServer.registerMBean(registry, registryName);
            registry.start();

            String url = "service:jmx:rmi://localhost";
            if (rmiTransportPort != PenroseClient.DEFAULT_RMI_TRANSPORT_PORT) url += ":"+rmiTransportPort;

            url += "/jndi/rmi://localhost";
            //if (rmiPort != PenroseClient.DEFAULT_RMI_PORT)
            url += ":"+rmiPort;

            url += "/jmx";

            JMXServiceURL serviceURL = new JMXServiceURL(url);
            jmxAuthenticator = new PenroseJMXAuthenticator(serviceContext.getPenroseServer().getPenrose());

            HashMap<String,Object> environment = new HashMap<String,Object>();
            environment.put("jmx.remote.authenticator", jmxAuthenticator);

            rmiConnector = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, environment, null);
            mbeanServer.registerMBean(rmiConnector, rmiConnectorName);
            rmiConnector.start();

            log.warn("Listening to port "+rmiPort+" (RMI).");
            if (rmiTransportPort != PenroseClient.DEFAULT_RMI_TRANSPORT_PORT) log.warn("Listening to port "+rmiTransportPort+" (RMI Transport).");
        }

        if (httpPort > 0) {
            xsltProcessor = new XSLTProcessor();
            mbeanServer.registerMBean(xsltProcessor, xsltProcessorName);

            httpConnector = new HttpAdaptor(httpPort, "localhost");
            httpConnector.setProcessorName(xsltProcessorName);
            mbeanServer.registerMBean(httpConnector, httpConnectorName);
            httpConnector.start();

            log.warn("Listening to port "+httpPort+" (HTTP).");
        }

        setStatus(STARTED);
    }

    public void stop() throws Exception {

        if (httpPort > 0) {
            httpConnector.stop();
            mbeanServer.unregisterMBean(httpConnectorName);
            mbeanServer.unregisterMBean(xsltProcessorName);
        }

        if (rmiPort > 0) { // && createdMBeanServer) {
            rmiConnector.stop();
            mbeanServer.unregisterMBean(rmiConnectorName);

            registry.stop();
            mbeanServer.unregisterMBean(registryName);
        }

        unregister();

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
        register(ObjectName.getInstance(name), object);
    }

    public void register(ObjectName objectName, Object object) throws Exception {
        log.debug("Registering "+objectName);
        if (mbeanServer.isRegistered(objectName)) mbeanServer.unregisterMBean(objectName);
        mbeanServer.registerMBean(object, objectName);
    }

    public void unregister(String name) throws Exception {
        unregister(ObjectName.getInstance(name));
    }

    public void unregister(ObjectName objectName) throws Exception {
        log.debug("Unregistering "+objectName);
        if (mbeanServer.isRegistered(objectName)) mbeanServer.unregisterMBean(objectName);
    }

    public void register() throws Exception {

        penroseService.register();

        registerConfigs();
        registerServices();
        registerPartitions();
    }

    public void unregister() throws Exception {

        unregisterPartitions();
        unregisterServices();
        unregisterConfigs();

        penroseService.unregister();
    }

    public void registerConfigs() throws Exception {

        Penrose penrose = serviceContext.getPenroseServer().getPenrose();
        PenroseConfig penroseConfig = penrose.getPenroseConfig();

        register("Penrose Config:type=PenroseConfig", penroseConfig);
        register("Penrose Config:type=SessionConfig", penroseConfig.getSessionConfig());

        Collection<SchemaConfig> schemaConfigs = penroseConfig.getSchemaConfigs();
        for (SchemaConfig schemaConfig : schemaConfigs) {
            register("Penrose Config:name=" + schemaConfig.getName() + ",type=SchemaConfig", schemaConfig);
        }

        Collection<AdapterConfig> adapterConfigs = penroseConfig.getAdapterConfigs();
        for (AdapterConfig adapterConfig : adapterConfigs) {
            register("Penrose Config:name=" + adapterConfig.getName() + ",type=AdapterConfig", adapterConfig);
        }

        Partitions partitions = penrose.getPartitions();
        for (Partition partition : partitions.getPartitions()) {
            register("Penrose Config:name=" + partition.getName() + ",type=Partition", partition);
        }
    }

    public void unregisterConfigs() throws Exception {

        Penrose penrose = serviceContext.getPenroseServer().getPenrose();
        PenroseConfig penroseConfig = penrose.getPenroseConfig();

        Partitions partitions = penrose.getPartitions();
        for (Partition partition : partitions.getPartitions()) {
            unregister("Penrose Config:name=" + partition.getName() + ",type=Partition");
        }

        Collection<AdapterConfig> adapterConfigs = penroseConfig.getAdapterConfigs();
        for (AdapterConfig adapterConfig : adapterConfigs) {
            unregister("Penrose Config:name=" + adapterConfig.getName() + ",type=AdapterConfig");
        }

        Collection<SchemaConfig> schemaConfigs = penroseConfig.getSchemaConfigs();
        for (SchemaConfig schemaConfig : schemaConfigs) {
            unregister("Penrose Config:name=" + schemaConfig.getName() + ",type=SchemaConfig");
        }

        unregister("Penrose Config:type=SessionConfig");
        unregister("Penrose Config:type=PenroseConfig");
    }

    public void registerServices() throws Exception {
        Penrose penrose = serviceContext.getPenroseServer().getPenrose();
        PenroseContext penroseContext = penrose.getPenroseContext();

        register("Penrose:service=SchemaManager", penroseContext.getSchemaManager());
        register("Penrose:service=PartitionConfigs", penrose.getPartitionConfigs());
        register("Penrose:service=Partitions", penrose.getPartitions());

        SessionContext sessionContext = penrose.getSessionContext();

        register("Penrose:service=SessionManager", sessionContext.getSessionManager());
        register("Penrose:service=Services", serviceContext.getPenroseServer().getServices());
    }

    public void unregisterServices() throws Exception {
        unregister("Penrose:service=Services");
        unregister("Penrose:service=SessionManager");
        unregister("Penrose:service=Partitions");
        unregister("Penrose:service=PartitionConfigs");
        unregister("Penrose:service=SchemaManager");
    }

    public void registerPartitions() throws Exception {

        Penrose penrose = serviceContext.getPenroseServer().getPenrose();
        Partitions partitions = penrose.getPartitions();

        for (Partition partition : partitions.getPartitions()) {

            //String name = "Penrose Config:name="+partition.getName()+",type=Partition";
            //register(name, partition);

            registerConnections(partition);
            //registerSources(partition);
            registerModules(partition);
        }
    }

    public void unregisterPartitions() throws Exception {

        Penrose penrose = serviceContext.getPenroseServer().getPenrose();
        Partitions partitions = penrose.getPartitions();

        for (Partition partition : partitions.getPartitions()) {

            unregisterModules(partition);
            //unregisterSources(partition);
            unregisterConnections(partition);

            //String name = "Penrose Config:name="+partition.getName()+",type=Partition";
            //unregister(name);
        }
    }

    public void registerConnections(Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();

        Collection<ConnectionConfig> connectionConfigs = partitionConfig.getConnectionConfigs().getConnectionConfigs();
        for (ConnectionConfig connectionConfig : connectionConfigs) {

            String name = "Penrose Config:name=" + connectionConfig.getName() + ",partition=" + partition.getName() + ",type=ConnectionConfig";
            register(name, connectionConfig);
        }
    }

    public void unregisterConnections(Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();

        Collection<ConnectionConfig> connectionConfigs = partitionConfig.getConnectionConfigs().getConnectionConfigs();
        for (ConnectionConfig connectionConfig : connectionConfigs) {

            String name = "Penrose Config:name=" + connectionConfig.getName() + ",partition=" + partition.getName() + ",type=ConnectionConfig";
            unregister(name);
        }
    }

    public void registerSources(Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<SourceConfig> sourceConfigs = partitionConfig.getSourceConfigs().getSourceConfigs();
        for (SourceConfig sourceConfig : sourceConfigs) {

            String name = "Penrose Config:name=" + sourceConfig.getName() + ",partition=" + partition.getName() + ",type=SourceConfig";
            register(name, sourceConfig);

            registerFields(partition, sourceConfig);
        }
    }

    public void unregisterSources(Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<SourceConfig> sourceConfigs = partitionConfig.getSourceConfigs().getSourceConfigs();
        for (SourceConfig sourceConfig : sourceConfigs) {

            unregisterFields(partition, sourceConfig);

            String name = "Penrose Config:name=" + sourceConfig.getName() + ",partition=" + partition.getName() + ",type=SourceConfig";
            unregister(name);
        }
    }

    public void registerFields(Partition partition, SourceConfig sourceConfig) throws Exception {

        Collection<FieldConfig> fieldConfigs = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fieldConfigs) {

            String name = "Penrose Config:name=" + fieldConfig.getName() + ",source=" + sourceConfig.getName() + ",partition=" + partition.getName() + ",type=FieldConfig";
            register(name, fieldConfig);
        }
    }

    public void unregisterFields(Partition partition, SourceConfig sourceConfig) throws Exception {

        Collection<FieldConfig> fieldConfigs = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fieldConfigs) {

            String name = "Penrose Config:name=" + fieldConfig.getName() + ",source=" + sourceConfig.getName() + ",partition=" + partition.getName() + ",type=FieldConfig";
            unregister(name);
        }
    }

    public void registerModules(Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<ModuleConfig> moduleConfigs = partitionConfig.getModuleConfigs().getModuleConfigs();
        for (ModuleConfig moduleConfig : moduleConfigs) {

            String name = "Penrose Config:name=" + moduleConfig.getName() + ",partition=" + partition.getName() + ",type=ModuleConfig";
            register(name, moduleConfig);
        }
    }

    public void unregisterModules(Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        Collection<ModuleConfig> moduleConfigs = partitionConfig.getModuleConfigs().getModuleConfigs();
        for (ModuleConfig moduleConfig : moduleConfigs) {

            String name = "Penrose Config:name=" + moduleConfig.getName() + ",partition=" + partition.getName() + ",type=ModuleConfig";
            unregister(name);
        }
    }
}
