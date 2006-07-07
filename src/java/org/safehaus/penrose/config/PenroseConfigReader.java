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
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.Reader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PenroseConfigReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    URL serverDtdUrl;
    Reader reader;

    public PenroseConfigReader(String filename) throws Exception {
        this(new FileReader(filename));
    }

    public PenroseConfigReader(Reader reader) {
        this.reader = reader;

        ClassLoader cl = getClass().getClassLoader();
        serverDtdUrl = cl.getResource("org/safehaus/penrose/config/server.dtd");
        //log.debug("Server DTD URL: "+serverDtdUrl);
    }

    public PenroseConfig read() throws Exception {
        PenroseConfig penroseConfig = new PenroseConfig();
        read(penroseConfig);
        return penroseConfig;
	}

    public void read(PenroseConfig penroseConfig) throws Exception {
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/server-digester-rules.xml");

		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
		digester.setValidating(true);
        digester.setClassLoader(cl);
		digester.push(penroseConfig);
		digester.parse(reader);
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        //log.debug("Resolving "+publicId+" "+systemId);

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);
        //log.debug("=> "+file);

        if ("server.dtd".equals(file)) {
            return new InputSource(serverDtdUrl.openStream());
        }

        return null;
    }
}
