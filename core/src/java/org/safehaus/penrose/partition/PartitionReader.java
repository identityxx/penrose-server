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

import org.safehaus.penrose.connection.ConnectionReader;
import org.safehaus.penrose.source.SourceReader;
import org.safehaus.penrose.module.ModuleReader;
import org.safehaus.penrose.mapping.MappingReader;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader {

    Logger log = LoggerFactory.getLogger(getClass());

    ConnectionReader connectionReader = new ConnectionReader();
    SourceReader sourceReader = new SourceReader();
    MappingReader mappingReader = new MappingReader();
    ModuleReader moduleReader = new ModuleReader();

    public PartitionReader() {
    }

    public Partition read(String path) throws Exception {
        PartitionConfigReader partitionConfigReader = new PartitionConfigReader();
        PartitionConfig partitionConfig = partitionConfigReader.read(path);
        return read(path, partitionConfig);
    }

    public Partition read(String path, PartitionConfig partitionConfig) throws Exception {
        Partition partition = new Partition(partitionConfig);
        connectionReader.read(path, partition);
        sourceReader.read(path, partition);
        mappingReader.read(path, partition);
        moduleReader.read(path, partition);
        return partition;
    }
}
