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

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.safehaus.penrose.schema.SchemaConfig;

import java.io.*;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PenroseConfigReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL penroseDtdUrl;
    Reader reader;

    File configPath;
    File schemaDir;

    public PenroseConfigReader(File configPath, File schemaDir) throws Exception {

        this.configPath = configPath;
        this.schemaDir = schemaDir;

        init(new FileReader(configPath));
    }

    public void init(Reader reader) throws Exception {
        this.reader = reader;

        ClassLoader cl = getClass().getClassLoader();
        penroseDtdUrl = cl.getResource("org/safehaus/penrose/config/server.dtd");
        //log.debug("Penrose DTD URL: "+penroseDtdUrl);
    }

    public PenroseConfig read() throws Exception {

        log.debug("Loading Penrose configuration: "+configPath);
        PenroseConfig penroseConfig = new PenroseConfig();
        read(penroseConfig);

        File[] schemaFiles = schemaDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".schema");
            }
        });

        log.debug("Schema files:");
        for (File schemaFile : schemaFiles) {
            String path = schemaFile.getPath();
            log.debug(" - "+path);

            SchemaConfig schemaConfig = new SchemaConfig(path);
            penroseConfig.addSchemaConfig(schemaConfig);
        }

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
            return new InputSource(penroseDtdUrl.openStream());
        }

        return null;
    }
}
