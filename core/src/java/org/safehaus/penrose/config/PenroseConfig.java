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
package org.safehaus.penrose.config;

import java.util.*;

import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.simple.SimpleEngine;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.user.UserConfig;
import org.safehaus.penrose.session.SessionConfig;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.handler.DefaultHandler;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.ldap.DN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PenroseConfig implements PenroseConfigMBean, Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    private String home;

    private Map<String,String> systemProperties              = new LinkedHashMap<String,String>();
    private Map<String,String> properties                    = new LinkedHashMap<String,String>();
    private Map<String,ServiceConfig> serviceConfigs         = new LinkedHashMap<String,ServiceConfig>();

    private Map<String,SchemaConfig> schemaConfigs           = new LinkedHashMap<String,SchemaConfig>();
    private Map<String,AdapterConfig> adapterConfigs         = new LinkedHashMap<String,AdapterConfig>();
    private Map<String,PartitionConfig> partitionConfigs     = new LinkedHashMap<String,PartitionConfig>();
    private Map<String,EngineConfig> engineConfigs           = new LinkedHashMap<String,EngineConfig>();
    private Map<String,HandlerConfig> handlerConfigs         = new LinkedHashMap<String,HandlerConfig>();
    private Map<String,InterpreterConfig> interpreterConfigs = new LinkedHashMap<String,InterpreterConfig>();

    private CacheConfig entryCacheConfig;

    private SessionConfig sessionConfig;
    private ConnectorConfig connectorConfig;

    private UserConfig rootUserConfig;

    public PenroseConfig() {

        entryCacheConfig = new CacheConfig();
        entryCacheConfig.setName(EntryCache.DEFAULT_CACHE_NAME);
        entryCacheConfig.setCacheClass(EntryCache.DEFAULT_CACHE_CLASS);

        connectorConfig = new ConnectorConfig();
        sessionConfig = new SessionConfig();

        rootUserConfig = new UserConfig("uid=admin,ou=system", "secret");

        init();
    }

    public void init() {
        addInterpreterConfig(new InterpreterConfig("DEFAULT", DefaultInterpreter.class.getName()));
        addHandlerConfig(new HandlerConfig("DEFAULT", DefaultHandler.class.getName()));
        addEngineConfig(new EngineConfig("DEFAULT", SimpleEngine.class.getName()));
    }

    public String getSystemProperty(String name) {
        return systemProperties.get(name);
    }

    public Map<String,String> getSystemProperties() {
        return systemProperties;
    }

    public Collection<String> getSystemPropertyNames() {
        return systemProperties.keySet();
    }

    public void setSystemProperty(String name, String value) {
        systemProperties.put(name, value);
    }

    public String removeSystemProperty(String name) {
        return systemProperties.remove(name);
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public Map getProperties() {
        return properties;
    }

    public Collection<String> getPropertyNames() {
        return properties.keySet();
    }

    public void setProperty(String name, String value) {
        properties.put(name, value);
    }

    public String removeProperty(String name) {
        return properties.remove(name);
    }

    public void addServiceConfig(ServiceConfig serviceConfig) {
        serviceConfigs.put(serviceConfig.getName(), serviceConfig);
    }

    public ServiceConfig getServiceConfig(String name) {
        return serviceConfigs.get(name);
    }

    public Collection<ServiceConfig> getServiceConfigs() {
        return serviceConfigs.values();
    }

    public Collection<String> getServiceNames() {
        return serviceConfigs.keySet();
    }

    public ServiceConfig removeServiceConfig(String name) {
        return serviceConfigs.remove(name);
    }

    public void addEngineConfig(EngineConfig engineConfig) {
        engineConfigs.put(engineConfig.getName(), engineConfig);
    }

    public EngineConfig getEngineConfig(String name) {
        return engineConfigs.get(name);
    }

    public Collection<EngineConfig> getEngineConfigs() {
        return engineConfigs.values();
    }

    public Collection<String> getEngineNames() {
        return engineConfigs.keySet();
    }

    public Collection<InterpreterConfig> getInterpreterConfigs() {
        return interpreterConfigs.values();
    }

    public void addInterpreterConfig(InterpreterConfig interpreterConfig) {
        interpreterConfigs.put(interpreterConfig.getName(), interpreterConfig);
    }

    public Collection<AdapterConfig> getAdapterConfigs() {
        return adapterConfigs.values();
    }

    public AdapterConfig getAdapterConfig(String name) {
        return adapterConfigs.get(name);
    }

    public void addAdapterConfig(AdapterConfig adapter) {
        adapterConfigs.put(adapter.getName(), adapter);
    }

    public Collection<String> getAdapterNames() {
        return adapterConfigs.keySet();
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public CacheConfig getEntryCacheConfig() {
        return entryCacheConfig;
    }

    public void setEntryCacheConfig(CacheConfig entryCacheConfig) {
        this.entryCacheConfig = entryCacheConfig;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public void addSchemaConfig(SchemaConfig schemaConfig) {
        schemaConfigs.put(schemaConfig.getName(), schemaConfig);
    }

    public SchemaConfig getSchemaConfig(String name) {
        return schemaConfigs.get(name);
    }

    public Collection<SchemaConfig> getSchemaConfigs() {
        return schemaConfigs.values();
    }

    public Collection<String> getSchemaNames() {
        return schemaConfigs.keySet();
    }

    public SchemaConfig removeSchemaConfig(String name) {
        return schemaConfigs.remove(name);
    }

    public void addPartitionConfig(PartitionConfig partitionConfig) {
        partitionConfigs.put(partitionConfig.getName(), partitionConfig);
    }

    public PartitionConfig getPartitionConfig(String name) {
        return partitionConfigs.get(name);
    }

    public Collection<PartitionConfig> getPartitionConfigs() {
        return partitionConfigs.values();
    }

    public Collection<String> getPartitionNames() {
        return partitionConfigs.keySet();
    }

    public PartitionConfig removePartitionConfig(String name) {
        return partitionConfigs.remove(name);
    }

    public HandlerConfig getHandlerConfig(String name) {
        return handlerConfigs.get(name);
    }

    public Collection<HandlerConfig> getHandlerConfigs() {
        return handlerConfigs.values();
    }

    public Collection<String> getHandlerNames() {
        return handlerConfigs.keySet();
    }

    public void addHandlerConfig(HandlerConfig handlerConfig) {
        //log.debug("Adding handler "+handlerConfig.getName()+": "+handlerConfig.getHandlerClass());
        handlerConfigs.put(handlerConfig.getName(), handlerConfig);
    }

    public HandlerConfig removeHandlerConfig(String name) {
        return handlerConfigs.remove(name);
    }

    public UserConfig getRootUserConfig() {
        return rootUserConfig;
    }

    public void setRootUserConfig(UserConfig rootUserConfig) {
        this.rootUserConfig = rootUserConfig;
    }

    public DN getRootDn() {
        return rootUserConfig.getDn();
    }

    public void setRootDn(String rootDn) {
        rootUserConfig.setDn(rootDn);
    }

    public void setRootDn(DN rootDn) {
        rootUserConfig.setDn(rootDn);
    }

    public byte[] getRootPassword() {
        return rootUserConfig.getPassword();
    }

    public void setRootPassword(String rootPassword) {
        rootUserConfig.setPassword(rootPassword);
    }

    public void setRootPassword(byte[] rootPassword) {
        rootUserConfig.setPassword(rootPassword);
    }

    public SessionConfig getSessionConfig() {
        return sessionConfig;
    }

    public void setSessionConfig(SessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
    }

    public int hashCode() {
        return (home == null ? 0 : home.hashCode()) +
                (systemProperties == null ? 0 : systemProperties.hashCode()) +
                (properties == null ? 0 : properties.hashCode()) +
                (serviceConfigs == null ? 0 : serviceConfigs.hashCode()) +
                (schemaConfigs == null ? 0 : schemaConfigs.hashCode()) +
                (adapterConfigs == null ? 0 : adapterConfigs.hashCode()) +
                (partitionConfigs == null ? 0 : partitionConfigs.hashCode()) +
                (handlerConfigs == null ? 0 : handlerConfigs.hashCode()) +
                (interpreterConfigs == null ? 0 : interpreterConfigs.hashCode()) +
                (entryCacheConfig == null ? 0 : entryCacheConfig.hashCode()) +
                (sessionConfig == null ? 0 : sessionConfig.hashCode()) +
                (engineConfigs == null ? 0 : engineConfigs.hashCode()) +
                (connectorConfig == null ? 0 : connectorConfig.hashCode()) +
                (rootUserConfig == null ? 0 : rootUserConfig.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        PenroseConfig penroseConfig = (PenroseConfig)object;

        if (!equals(home, penroseConfig.home)) return false;

        if (!equals(systemProperties, penroseConfig.systemProperties)) return false;
        if (!equals(properties, penroseConfig.properties)) return false;
        if (!equals(serviceConfigs, penroseConfig.serviceConfigs)) return false;

        if (!equals(schemaConfigs, penroseConfig.schemaConfigs)) return false;
        if (!equals(adapterConfigs, penroseConfig.adapterConfigs)) return false;
        if (!equals(partitionConfigs, penroseConfig.partitionConfigs)) return false;
        if (!equals(handlerConfigs, penroseConfig.handlerConfigs)) return false;
        if (!equals(interpreterConfigs, penroseConfig.interpreterConfigs)) return false;

        if (!equals(entryCacheConfig, penroseConfig.entryCacheConfig)) return false;

        if (!equals(sessionConfig, penroseConfig.sessionConfig)) return false;
        if (!equals(engineConfigs, penroseConfig.engineConfigs)) return false;
        if (!equals(connectorConfig, penroseConfig.connectorConfig)) return false;

        if (!equals(rootUserConfig, penroseConfig.rootUserConfig)) return false;

        return true;
    }

    public void copy(PenroseConfig penroseConfig) throws CloneNotSupportedException {

        home = penroseConfig.home;

        systemProperties = new LinkedHashMap<String,String>();
        systemProperties.putAll(penroseConfig.systemProperties);

        properties = new LinkedHashMap<String,String>();
        properties.putAll(penroseConfig.properties);

        serviceConfigs = new LinkedHashMap<String,ServiceConfig>();
        for (ServiceConfig serviceConfig : penroseConfig.serviceConfigs.values()) {
            addServiceConfig((ServiceConfig) serviceConfig.clone());
        }

        schemaConfigs = new LinkedHashMap<String,SchemaConfig>();
        for (SchemaConfig schemaConfig : penroseConfig.schemaConfigs.values()) {
            addSchemaConfig((SchemaConfig) schemaConfig.clone());
        }

        adapterConfigs = new LinkedHashMap<String,AdapterConfig>();
        for (AdapterConfig adapterConfig : penroseConfig.adapterConfigs.values()) {
            addAdapterConfig((AdapterConfig) adapterConfig.clone());
        }

        partitionConfigs = new LinkedHashMap<String,PartitionConfig>();
        for (PartitionConfig partitionConfig : penroseConfig.partitionConfigs.values()) {
            addPartitionConfig((PartitionConfig) partitionConfig.clone());
        }

        engineConfigs = new LinkedHashMap<String,EngineConfig>();
        for (EngineConfig engineConfig : penroseConfig.engineConfigs.values()) {
            addEngineConfig((EngineConfig) engineConfig.clone());
        }

        handlerConfigs = new LinkedHashMap<String,HandlerConfig>();
        for (HandlerConfig handlerConfig : penroseConfig.handlerConfigs.values()) {
            addHandlerConfig((HandlerConfig) handlerConfig.clone());
        }

        interpreterConfigs = new LinkedHashMap<String,InterpreterConfig>();
        for (InterpreterConfig interpreterConfig : penroseConfig.interpreterConfigs.values()) {
            addInterpreterConfig((InterpreterConfig) interpreterConfig.clone());
        }

        entryCacheConfig = (CacheConfig)penroseConfig.entryCacheConfig.clone();

        sessionConfig = (SessionConfig)penroseConfig.sessionConfig.clone();
        connectorConfig = (ConnectorConfig)penroseConfig.connectorConfig.clone();

        rootUserConfig = (UserConfig)penroseConfig.rootUserConfig.clone();
    }

    public void clear() {
        systemProperties.clear();
        properties.clear();
        serviceConfigs.clear();
        schemaConfigs.clear();
        adapterConfigs.clear();
        partitionConfigs.clear();
        engineConfigs.clear();
        handlerConfigs.clear();
        interpreterConfigs.clear();

        init();
    }

    public Object clone() throws CloneNotSupportedException {
        PenroseConfig penroseConfig = (PenroseConfig)super.clone();
        penroseConfig.copy(this);
        return penroseConfig;
    }
}
