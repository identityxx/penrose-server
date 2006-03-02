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
import org.apache.directory.server.configuration.MutableServerStartupConfiguration;
import org.apache.directory.server.ldap.support.extended.GracefulShutdownHandler;
import org.apache.directory.server.ldap.support.extended.LaunchDiagnosticUiHandler;
import org.apache.directory.server.jndi.ServerContextFactory;
import org.apache.directory.server.core.configuration.ShutdownConfiguration;
import org.apache.directory.server.core.jndi.CoreContextFactory;
import org.safehaus.penrose.PenroseServer;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.connector.AdapterConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.service.ServiceManager;
import org.safehaus.penrose.config.PenroseConfig;

import javax.naming.directory.InitialDirContext;
import javax.naming.*;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PenroseService implements PenroseServiceMBean {

    Logger log = Logger.getLogger(getClass());

    private String home;
    PenroseServer penroseServer;

    MBeanServer mbeanServer;

    public void create() throws Exception {
        log.info("Creating Penrose Service...");

        penroseServer = new PenroseServer(home);

        PenroseConfig config = penroseServer.getPenroseConfig();
        ServiceConfig jmxService = config.getServiceConfig("JMX");
        jmxService.setEnabled(false);

        Collection servers = MBeanServerFactory.findMBeanServer(null);
        if (!servers.isEmpty()) {
            mbeanServer = (MBeanServer)servers.iterator().next();
        }

        log.info("Penrose Service created.");
    }

    public void start() {

        try {
            log.info("Starting Penrose Service...");

            penroseServer.start();

            register();
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
            log.info("Penrose Service started.");

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
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
        registerServices();
        registerPartitions();
    }

    public void unregister() throws Exception {

        unregisterPartitions();
        unregisterServices();
        unregisterConfigs();
    }

    public void registerConfigs() throws Exception {

        Penrose penrose = penroseServer.getPenrose();
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

        Penrose penrose = penroseServer.getPenrose();
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

    public void registerServices() throws Exception {
        Penrose penrose = penroseServer.getPenrose();

        register("Penrose:service=SchemaManager", penrose.getSchemaManager());
        register("Penrose:service=ConnectionManager", penrose.getConnectionManager());
        register("Penrose:service=PartitionManager", penrose.getPartitionManager());
        register("Penrose:service=ModuleManager", penrose.getModuleManager());
        register("Penrose:service=SessionManager", penrose.getSessionManager());
        register("Penrose:service=ServiceManager", penroseServer.getServiceManager());
    }

    public void unregisterServices() throws Exception {
        unregister("Penrose:service=ServiceManager");
        unregister("Penrose:service=SessionManager");
        unregister("Penrose:service=ModuleManager");
        unregister("Penrose:service=PartitionManager");
        unregister("Penrose:service=ConnectionManager");
        unregister("Penrose:service=SchemaManager");
    }

    public void registerPartitions() throws Exception {

        Penrose penrose = penroseServer.getPenrose();
        PartitionManager partitionManager = penrose.getPartitionManager();

        Collection partitions = partitionManager.getPartitions();
        for (Iterator i=partitions.iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            //String name = "Penrose Config:name="+partition.getName()+",type=Partition";
            //register(name, partition);

            registerConnections(partition);
            //registerSources(partition);
            registerModules(partition);
        }
    }

    public void unregisterPartitions() throws Exception {

        Penrose penrose = penroseServer.getPenrose();
        PartitionManager partitionManager = penrose.getPartitionManager();

        Collection partitions = partitionManager.getPartitions();
        for (Iterator i=partitions.iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            unregisterModules(partition);
            //unregisterSources(partition);
            unregisterConnections(partition);

            //String name = "Penrose Config:name="+partition.getName()+",type=Partition";
            //unregister(name);
        }
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

    public void stop() {

        try {
            log.info("Stopping Penrose Service...");

            unregister();

            penroseServer.stop();
/*
            Hashtable env = new ShutdownConfiguration().toJndiEnvironment();
            env.put(Context.INITIAL_CONTEXT_FACTORY, CoreContextFactory.class.getName());
            env.put(Context.PROVIDER_URL, "ou=system");
            env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
            env.put(Context.SECURITY_CREDENTIALS, "system");
            env.put(Context.SECURITY_AUTHENTICATION, "simple");

            new InitialDirContext(env);
*/
            log.info("Penrose Service stopped.");

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void destroy() {
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public String getProductName() {
        return Penrose.PRODUCT_NAME;
    }

    public String getProductVersion() {
        return Penrose.PRODUCT_VERSION;
    }

    public void load() throws Exception {
        penroseServer.load(home);
    }

    public void store() throws Exception {
        penroseServer.store();
    }
}