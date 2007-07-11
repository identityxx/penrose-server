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

/**
 * @author Endi S. Dewata
 */
public class PartitionReader {

    Logger log = LoggerFactory.getLogger(getClass());

    ConnectionReader connectionReader;
    SourceReader sourceReader;
    MappingReader mappingReader;
    ModuleReader moduleReader;

    public PartitionReader() {
        this(null);
    }

    public PartitionReader(String home) {
        connectionReader = new ConnectionReader(home);
        sourceReader = new SourceReader(home);
        mappingReader = new MappingReader(home);
        moduleReader = new ModuleReader(home);
    }

    public Partition read(PartitionConfig partitionConfig) throws Exception {
        return read(partitionConfig, partitionConfig.getPath());
    }

    public Partition read(PartitionConfig partitionConfig, String path) throws Exception {
        Partition partition = new Partition(partitionConfig);

        Connections connections = partition.getConnections();
        connectionReader.read(path, connections);

        Sources sources = partition.getSources();
        sourceReader.read(path, sources);

        mappingReader.read(path, partition);

        Modules modules = partition.getModules();
        moduleReader.read(path, modules);

        return partition;
    }
}
