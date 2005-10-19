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

import java.io.File;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class ServerConfigReader {

    Logger log = Logger.getLogger(getClass());

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
    public void read(String filename) throws Exception {
        File file = new File(filename);
        read(file);
    }

	/**
	 * Load server configuration from a file
	 *
	 * @param file the configuration file (ie. server.xml)
	 * @throws Exception
	 */
	public void read(File file) throws Exception {
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
