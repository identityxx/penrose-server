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

import java.io.Reader;
import java.io.FileReader;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PenroseConfigReader {

    Logger log = Logger.getLogger(getClass());

    Reader reader;

    public PenroseConfigReader(String filename) throws Exception {
        reader = new FileReader(filename);
    }

    public PenroseConfigReader(Reader reader) {
        this.reader = reader;
    }

    /**
     * Load server configuration from a file
     *
     * @throws Exception
     */
    public PenroseConfig read() throws Exception {
        PenroseConfig penroseConfig = new PenroseConfig();

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/server-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(penroseConfig);
		digester.parse(reader);

        return penroseConfig;
	}
}
