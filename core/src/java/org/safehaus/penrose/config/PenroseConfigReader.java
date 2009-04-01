/**
 * Copyright 2009 Red Hat, Inc.
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
import org.safehaus.penrose.PenroseConfig;

import java.io.*;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PenroseConfigReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    ClassLoader classLoader;
    URL penroseDtdUrl;
    URL digesterUrl;

    public PenroseConfigReader() throws Exception {

        classLoader   = getClass().getClassLoader();
        penroseDtdUrl = classLoader.getResource("org/safehaus/penrose/config/server.dtd");
        digesterUrl   = classLoader.getResource("org/safehaus/penrose/config/server-digester-rules.xml");
    }

    public PenroseConfig read(File path) throws Exception {

        log.debug("Loading Penrose configuration: "+ path);

        PenroseConfig penroseConfig = new PenroseConfig();

        Reader reader = new FileReader(path);

		Digester digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
		digester.setValidating(true);
        digester.setClassLoader(classLoader);
		digester.push(penroseConfig);
		digester.parse(reader);

        return penroseConfig;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        //log.debug("Resolving "+publicId+" "+systemId);

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);
        //log.debug("=> "+file);

        if ("server.dtd".equals(file)) {
            return new InputSource(penroseDtdUrl.openStream());
        }

        return null;
    }
}
