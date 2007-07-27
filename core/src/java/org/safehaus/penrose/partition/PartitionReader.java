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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connection.ConnectionReader;
import org.safehaus.penrose.connection.Connections;
import org.safehaus.penrose.source.SourceReader;
import org.safehaus.penrose.source.Sources;
import org.safehaus.penrose.module.ModuleReader;
import org.safehaus.penrose.module.Modules;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    ConnectionReader connectionReader;
    SourceReader     sourceReader;
    MappingReader    mappingReader;
    ModuleReader     moduleReader;

    public PartitionReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/partition/partition.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/partition/partition-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);

        connectionReader = new ConnectionReader();
        sourceReader     = new SourceReader();
        mappingReader    = new MappingReader();
        moduleReader     = new ModuleReader();
    }

    public PartitionConfig read(String dir) throws Exception {
        return read(new File(dir));
    }

    public PartitionConfig read(File baseDir) throws Exception {

        PartitionConfig partitionConfig = new PartitionConfig();

        File dirInf = new File(baseDir, "DIR-INF");

        File partitionXml = new File(dirInf, "partition.xml");
        if (partitionXml.exists()) {
            log.debug("Loading "+partitionXml.getAbsolutePath()+".");
            digester.push(partitionConfig);
            digester.parse(partitionXml);
            digester.pop();
        } else {
            partitionConfig.setName(baseDir.getName());
        }

        //log.debug("Classpath:");

        File classesDir = new File(dirInf, "classes");
        if (classesDir.isDirectory()) {
            URL url = classesDir.toURL();
            //log.debug(" - "+url);
            partitionConfig.addClassPath(url);
        }

        File libDir = new File(dirInf, "lib");
        if (libDir.isDirectory()) {
            File files[] = libDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for (File f : files) {
                URL url = f.toURL();
                //log.debug(" - "+url);
                partitionConfig.addClassPath(url);
            }
        }

        File connectionsXml = new File(dirInf, "connections.xml");
        if (connectionsXml.exists()) {
            log.debug("Loading "+connectionsXml.getAbsolutePath()+".");
            Connections connections = partitionConfig.getConnections();
            connectionReader.read(connectionsXml, connections);
        }

        File sourcesXml = new File(dirInf, "sources.xml");
        if (sourcesXml.exists()) {
            log.debug("Loading "+sourcesXml.getAbsolutePath()+".");
            Sources sources = partitionConfig.getSources();
            sourceReader.read(sourcesXml, sources);
        }

        File mappingXml = new File(dirInf, "mapping.xml");
        if (mappingXml.exists()) {
            log.debug("Loading "+mappingXml.getAbsolutePath()+".");
            Mappings mappings = partitionConfig.getMappings();
            mappingReader.read(mappingXml, mappings);
        }

        File modulesFile = new File(dirInf, "modules.xml");
        if (modulesFile.exists()) {
            log.debug("Loading "+modulesFile.getAbsolutePath()+".");
            Modules modules = partitionConfig.getModules();
            moduleReader.read(modulesFile, modules);
        }

        return partitionConfig;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
