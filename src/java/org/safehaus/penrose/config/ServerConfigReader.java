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

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.log4j.Logger;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;

import java.io.File;
import java.net.URL;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ServerConfigReader {

    Logger log = Logger.getLogger(getClass());

    public ServerConfigReader() {
    }

    /**
     * Load server configuration from a file
     *
     * @param filename the configuration file (ie. server.xml)
     * @throws Exception
     */
    public ServerConfig read(String filename) throws Exception {
        ServerConfig serverConfig = new ServerConfig();
        File file = new File(filename);
        read(file, serverConfig);
        return serverConfig;
    }

	/**
	 * Load server configuration from a file
	 *
	 * @param file the configuration file (ie. server.xml)
	 * @throws Exception
	 */
	public void read(File file, ServerConfig serverConfig) throws Exception {
		//log.debug("Loading server configuration file from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/server-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(serverConfig);
		digester.parse(file);

        if (serverConfig.getInterpreterConfigs().isEmpty()) {
            InterpreterConfig interpreterConfig = new InterpreterConfig();
            serverConfig.addInterpreterConfig(interpreterConfig);
        }

        if (serverConfig.getConnectorConfigs().isEmpty()) {
            ConnectorConfig connectorConfig = new ConnectorConfig();
            serverConfig.addConnectorConfig(connectorConfig);
        }

        for (Iterator i=serverConfig.getConnectorConfigs().iterator(); i.hasNext(); ) {
            ConnectorConfig connectorConfig = (ConnectorConfig)i.next();

            CacheConfig dataCacheConfig = connectorConfig.getCacheConfig(ConnectorConfig.CACHE);

            if (dataCacheConfig == null) {
                dataCacheConfig = new CacheConfig();
                dataCacheConfig.setCacheName(ConnectorConfig.CACHE);
                dataCacheConfig.setCacheClass(CacheConfig.DEFAULT_CONNECTOR_CACHE);
                connectorConfig.addCacheConfig(dataCacheConfig);
            }

        }

        if (serverConfig.getEngineConfigs().isEmpty()) {
            EngineConfig engineConfig = new EngineConfig();
            serverConfig.addEngineConfig(engineConfig);
        }

        for (Iterator i=serverConfig.getEngineConfigs().iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();

            CacheConfig cacheconfig = engineConfig.getCacheConfig(EngineConfig.CACHE);

            if (cacheconfig == null) {
                cacheconfig = new CacheConfig();
                cacheconfig.setCacheName(EngineConfig.CACHE);
                cacheconfig.setCacheClass(CacheConfig.DEFAULT_ENGINE_CACHE);
                engineConfig.addCacheConfig(cacheconfig);
            }

        }

	}
}
