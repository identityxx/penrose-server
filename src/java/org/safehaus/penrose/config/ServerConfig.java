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

import java.io.Serializable;
import java.util.*;

import org.apache.log4j.Logger;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.connector.ConnectorConfig;


/**
 * @author Endi S. Dewata
 */
public class ServerConfig implements Serializable {

    Logger log = Logger.getLogger(getClass());

    private Collection schemaFiles = new ArrayList();

    private String rootDn;
    private String rootPassword;
	
    private Map interpreterConfigs = new LinkedHashMap();
    private Map cacheConfigs = new LinkedHashMap();
    private Map engineConfigs = new LinkedHashMap();
    private Map connectorConfigs = new LinkedHashMap();
    private Map adapterConfigs = new LinkedHashMap();

    public ServerConfig() {
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

	public String toString() {

		String nl = System.getProperty("line.separator");
		StringBuffer sb = new StringBuffer();
		
        sb.append(nl);
        sb.append(nl);

        sb.append("ENGINE:");
        sb.append(nl);
        sb.append(nl);

        for (Iterator i = engineConfigs.keySet().iterator(); i.hasNext();) {
            String engineName = (String) i.next();
            EngineConfig engineConfig = (EngineConfig)engineConfigs.get(engineName);
            sb.append(" - "+engineName + " (" + engineConfig.getEngineClass() + ")" + nl);
            sb.append("   Parameters:" + nl);
            for (Iterator j = engineConfig.getParameterNames().iterator(); j.hasNext();) {
                String name = (String) j.next();
                String value = engineConfig.getParameter(name);
                sb.append("   - " + name + ": " + value + nl);
            }
            sb.append(nl);
        }

        sb.append(nl);

        sb.append("CACHE:");
        sb.append(nl);
        sb.append(nl);

        for (Iterator i = cacheConfigs.keySet().iterator(); i.hasNext();) {
            String cacheName = (String) i.next();
            CacheConfig sourceCache = (CacheConfig) cacheConfigs.get(cacheName);
            sb.append(" - "+ cacheName + " (" + sourceCache.getCacheClass() + ")" + nl);
            sb.append("   Parameters:" + nl);
            for (Iterator j = sourceCache.getParameterNames().iterator(); j.hasNext();) {
                String name = (String) j.next();
                String value = sourceCache.getParameter(name);
                sb.append("   - " + name + ": " + value + nl);
            }
            sb.append(nl);
        }

        sb.append("ADAPTERS:");
        sb.append(nl);
        sb.append(nl);

        for (Iterator i = adapterConfigs.keySet().iterator(); i.hasNext();) {
            String adapterName = (String) i.next();
            AdapterConfig adapterClass = (AdapterConfig) adapterConfigs.get(adapterName);
            sb.append(" - "+ adapterName + " (" + adapterClass.getAdapterClass() + ")" + nl);
            sb.append("   Parameters:" + nl);
            for (Iterator j = adapterClass.getParameterNames().iterator(); j.hasNext();) {
                String name = (String) j.next();
                String value = adapterClass.getParameter(name);
                sb.append("   - " + name + ": " + value + nl);
            }
            sb.append(nl);
        }

		return sb.toString();
	}

    public Collection getAdapterConfigs() {
        return adapterConfigs.values();
    }

    public AdapterConfig getAdapterConfig(String name) {
        return (AdapterConfig)adapterConfigs.get(name);
    }

    public Collection getSchemaFiles() {
        return schemaFiles;
    }

    public void setSchemaFiles(List schemaFiles) {
        this.schemaFiles = schemaFiles;
    }
    
    public void addCacheConfig(CacheConfig cacheConfig) {
    	cacheConfigs.put(cacheConfig.getCacheName(), cacheConfig);
    }

    public CacheConfig removeCacheConfig(String name) {
        return (CacheConfig)cacheConfigs.remove(name);
    }

    public CacheConfig getCacheConfig() {
        return (CacheConfig)cacheConfigs.get("DEFAULT");
    }

    public CacheConfig getCacheConfig(String name) {
        return (CacheConfig)cacheConfigs.get(name);
    }

    public Collection getCacheConfigs() {
    	return cacheConfigs.values();
    }

    public void addEngineConfig(EngineConfig engineConfig) {
        engineConfigs.put(engineConfig.getEngineName(), engineConfig);
    }

    public Collection getEngineConfigs() {
        return engineConfigs.values();
    }

    public void addInterpreterConfig(InterpreterConfig interpreterConfig) {
        interpreterConfigs.put(interpreterConfig.getInterpreterName(), interpreterConfig);
    }

    public Collection getInterpreterConfigs() {
        return interpreterConfigs.values();
    }

    public InterpreterConfig getInterpreterConfig(String name) {
        return (InterpreterConfig)interpreterConfigs.get(name);
    }

    public void setAdapterConfigs(Map adapterConfigs) {
        this.adapterConfigs = adapterConfigs;
    }

    public void addAdapterConfig(AdapterConfig adapter) {
        adapterConfigs.put(adapter.getAdapterName(), adapter);
    }

    public void setCacheConfigs(Map cacheConfigs) {
        this.cacheConfigs = cacheConfigs;
    }

    public void setInterpreterConfigs(Map interpreterConfigs) {
        this.interpreterConfigs = interpreterConfigs;
    }

    public void addConnectorConfig(ConnectorConfig connectorConfig) {
        connectorConfigs.put(connectorConfig.getConnectorName(), connectorConfig);
    }

    public ConnectorConfig getConnectorConfig(String connectorName) {
        return (ConnectorConfig)connectorConfigs.get(connectorName);
    }

    public Collection getConnectorConfigs() {
        return connectorConfigs.values();
    }

    public ConnectorConfig removeConnectorConfig(String connectorName) {
        return (ConnectorConfig)connectorConfigs.remove(connectorName);
    }

    public void setConnectorConfigs(Map connectorConfigs) {
        this.connectorConfigs = connectorConfigs;
    }
}
