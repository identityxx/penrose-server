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
import java.net.URLClassLoader;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    ConnectionReader connectionReader;
    SourceReader sourceReader;
    MappingReader mappingReader;
    ModuleReader moduleReader;

    public PartitionReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/partition/partition.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/partition/partition-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);

        connectionReader = new ConnectionReader();
        sourceReader = new SourceReader();
        mappingReader = new MappingReader();
        moduleReader = new ModuleReader();
    }

    public Partition read(String dir) throws Exception {
        return read(new File(dir));
    }

    public Partition read(File dir) throws Exception {

        String base = dir.getAbsolutePath()+File.separator+"DIR-INF";

        PartitionConfig partitionConfig = new PartitionConfig();

        String partitionFile = base+File.separator+"partition.xml";
        File file = new File(partitionFile);

        if (file.exists()) {
            digester.push(partitionConfig);
            digester.parse(file);
            digester.pop();
        } else {
            partitionConfig.setName(dir.getName());
        }

        String connectionsFile = base+File.separator+"connections.xml";
        log.debug("Loading "+connectionsFile);

        Connections connections = partitionConfig.getConnections();
        connectionReader.read(connectionsFile, connections);

        String sourcesFile = base+File.separator+"sources.xml";
        log.debug("Loading "+sourcesFile);

        Sources sources = partitionConfig.getSources();
        sourceReader.read(sourcesFile, sources);

        String mappingFile = base+File.separator+"mapping.xml";
        log.debug("Loading "+mappingFile);

        Mappings mappings = partitionConfig.getMappings();
        mappingReader.read(mappingFile, mappings);

        String modulesFile = base+File.separator+"modules.xml";
        log.debug("Loading "+modulesFile);

        Modules modules = partitionConfig.getModules();
        moduleReader.read(modulesFile, modules);

        Partition partition = new Partition(partitionConfig);

        log.debug("Classpath:");
        List<URL> urls = new ArrayList<URL>();

        File classesDir = new File(base+File.separator+"classes");
        if (classesDir.exists()) {
            URL url = classesDir.toURL();
            log.debug(" - "+url);
            urls.add(url);
        }

        File libDir = new File(base+File.separator+"lib");
        if (libDir.isDirectory()) {
            File files[] = libDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for (File f : files) {
                URL url = f.toURL();
                log.debug(" - "+url);
                urls.add(url);
            }
        }

        URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
        partition.setClassLoader(classLoader);

        return partition;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
