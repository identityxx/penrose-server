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
import org.safehaus.penrose.module.ModuleWriter;
import org.safehaus.penrose.connection.ConnectionWriter;
import org.safehaus.penrose.source.SourceWriter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PartitionWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    ConnectionWriter connectionWriter = new ConnectionWriter();
    SourceWriter sourceWriter = new SourceWriter();
    MappingWriter mappingWriter = new MappingWriter();
    ModuleWriter moduleWriter = new ModuleWriter();

    public PartitionWriter() {
    }

    public void write(String directory, Partition partition) throws Exception {
        connectionWriter.write(directory, partition);
        sourceWriter.write(directory, partition);
        mappingWriter.write(directory, partition);
        moduleWriter.write(directory, partition);
    }
}
