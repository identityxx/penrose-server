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

import java.io.File;
import java.io.FilenameFilter;
import java.net.URLClassLoader;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader {

    Logger log = LoggerFactory.getLogger(getClass());

    String home;

    ConnectionReader connectionReader;
    SourceReader sourceReader;
    MappingReader mappingReader;
    ModuleReader moduleReader;

    public PartitionReader() {
        this(null);
    }

    public PartitionReader(String home) {
        this.home = home;

        connectionReader = new ConnectionReader();
        sourceReader = new SourceReader();
        mappingReader = new MappingReader();
        moduleReader = new ModuleReader();
    }

    public Partition read(PartitionConfig partitionConfig) throws Exception {
        return read(partitionConfig, partitionConfig.getPath());
    }

    public Partition read(PartitionConfig partitionConfig, String path) throws Exception {
        Partition partition = new Partition(partitionConfig);

        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+ File.separator+path;
        }

        String base = (path == null ? "" : path+File.separator)+"DIR-INF";

        String connectionsFile = base+File.separator+"connections.xml";
        log.debug("Loading "+connectionsFile);

        Connections connections = partition.getConnections();
        connectionReader.read(connectionsFile, connections);

        String sourcesFile = base+File.separator+"sources.xml";
        log.debug("Loading "+sourcesFile);

        Sources sources = partition.getSources();
        sourceReader.read(sourcesFile, sources);

        String mappingFile = base+File.separator+"mapping.xml";
        log.debug("Loading "+mappingFile);

        Mappings mappings = partition.getMappings();
        mappingReader.read(mappingFile, mappings);

        String modulesFile = base+File.separator+"modules.xml";
        log.debug("Loading "+modulesFile);

        Modules modules = partition.getModules();
        moduleReader.read(modulesFile, modules);

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

            for (File file : files) {
                URL url = file.toURL();
                log.debug(" - "+url);
                urls.add(url);
            }
        }

        URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
        partition.setClassLoader(classLoader);

        return partition;
    }
}
