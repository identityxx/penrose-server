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
package org.safehaus.penrose.config;

import java.util.*;

import org.apache.log4j.Logger;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.connector.AdapterConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.schema.SchemaConfig;


/**
 * @author Endi S. Dewata
 */
public class PenroseConfig {

    Logger log = Logger.getLogger(getClass());

    private int port = 10389;
    private int securePort = 10639;

    private int jmxRmiPort = 1099;
    private int jmxHttpPort = 8112;

    private String home;

    private String rootDn = "uid=admin,ou=system";
    private String rootPassword = "secret";
	
    private Map systemProperties = new LinkedHashMap();
    private Map schemaConfigs    = new LinkedHashMap();
    private Map adapterConfigs   = new LinkedHashMap();

    private InterpreterConfig interpreterConfig;

    private CacheConfig entryCacheConfig;
    private CacheConfig sourceCacheConfig;

    private ConnectorConfig connectorConfig;
    private EngineConfig engineConfig;

    private Map partitionConfigs = new LinkedHashMap();

    public PenroseConfig() {

        interpreterConfig = new InterpreterConfig();

        sourceCacheConfig = new CacheConfig();
        sourceCacheConfig.setCacheName(ConnectorConfig.DEFAULT_CACHE_NAME);
        sourceCacheConfig.setCacheClass(ConnectorConfig.DEFAULT_CACHE_CLASS);

        entryCacheConfig = new CacheConfig();
        entryCacheConfig.setCacheName(EngineConfig.DEFAULT_CACHE_NAME);
        entryCacheConfig.setCacheClass(EngineConfig.DEFAULT_CACHE_CLASS);

        connectorConfig = new ConnectorConfig();

        engineConfig = new EngineConfig();
    }

	/**
	 * @return Returns the rootDn.
	 */
	public String getRootDn() {
		return rootDn;
	}
	/**
	 * @param rootDn
	 *            The rootDn to set.
	 */
	public void setRootDn(String rootDn) {
		this.rootDn = rootDn;
	}
	/**
	 * @return Returns the rootPassword.
	 */
	public String getRootPassword() {
		return rootPassword;
	}
	/**
	 * @param rootPassword
	 *            The rootPassword to set.
	 */
	public void setRootPassword(String rootPassword) {
		this.rootPassword = rootPassword;
	}

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public EngineConfig getEngineConfig() {
        return engineConfig;
    }

    public void setInterpreterConfig(InterpreterConfig interpreterConfig) {
        this.interpreterConfig = interpreterConfig;
    }

    public InterpreterConfig getInterpreterConfig() {
        return interpreterConfig;
    }

    public Collection getAdapterConfigs() {
        return adapterConfigs.values();
    }

    public AdapterConfig getAdapterConfig(String name) {
        return (AdapterConfig)adapterConfigs.get(name);
    }

    public void setAdapterConfigs(Map adapterConfigs) {
        this.adapterConfigs = adapterConfigs;
    }

    public void addAdapterConfig(AdapterConfig adapter) {
        adapterConfigs.put(adapter.getAdapterName(), adapter);
    }

    public String getSystemProperty(String name) {
        return (String)systemProperties.get(name);
    }

    public Collection getSystemPropertyNames() {
        return systemProperties.keySet();
    }
    
    public void setSystemProperty(String name, String value) {
        systemProperties.put(name, value);
    }

    public String removeSystemProperty(String name) {
        return (String)systemProperties.remove(name);
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }
    
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getSecurePort() {
        return securePort;
    }

    public void setSecurePort(int securePort) {
        this.securePort = securePort;
    }

    public int getJmxRmiPort() {
        return jmxRmiPort;
    }

    public void setJmxRmiPort(int jmxRmiPort) {
        this.jmxRmiPort = jmxRmiPort;
    }

    public int getJmxHttpPort() {
        return jmxHttpPort;
    }

    public void setJmxHttpPort(int jmxHttpPort) {
        this.jmxHttpPort = jmxHttpPort;
    }

    public CacheConfig getEntryCacheConfig() {
        return entryCacheConfig;
    }

    public void setEntryCacheConfig(CacheConfig entryCacheConfig) {
        this.entryCacheConfig = entryCacheConfig;
    }

    public CacheConfig getSourceCacheConfig() {
        return sourceCacheConfig;
    }

    public void setSourceCacheConfig(CacheConfig sourceCacheConfig) {
        this.sourceCacheConfig = sourceCacheConfig;
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
        return (SchemaConfig)schemaConfigs.get(name);
    }

    public Collection getSchemaConfigs() {
        return schemaConfigs.values();
    }

    public SchemaConfig removeSchemaConfig(String name) {
        return (SchemaConfig)schemaConfigs.remove(name);
    }

    public void addPartitionConfig(PartitionConfig partitionConfig) {
        partitionConfigs.put(partitionConfig.getName(), partitionConfig);
    }

    public PartitionConfig getPartitionConfig(String name) {
        return (PartitionConfig)partitionConfigs.get(name);
    }

    public Collection getPartitionConfigs() {
        return partitionConfigs.values();
    }

    public PartitionConfig removePartitionConfig(String name) {
        return (PartitionConfig)partitionConfigs.remove(name);
    }
}
