/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.config;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.MappingRule;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ServerConfigReader {

    public Logger log = Logger.getLogger(Penrose.CONFIG_LOGGER);

    private ServerConfig serverConfig;

    public ServerConfigReader() {
        serverConfig = new ServerConfig();
    }

    public ServerConfigReader(ServerConfig config) {
        this.serverConfig = config;
    }

    /**
     * Load server configuration from a file
     *
     * @param filename the configuration file (ie. server.xml)
     * @throws Exception
     */
    public void loadServerConfig(String filename) throws Exception {
        File file = new File(filename);
        loadServerConfig(file);
    }

	/**
	 * Load server configuration from a file
	 *
	 * @param file the configuration file (ie. server.xml)
	 * @throws Exception
	 */
	public void loadServerConfig(File file) throws Exception {
		log.debug("Loading server configuration file from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/server-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(serverConfig);
		digester.parse(file);
	}

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

}
