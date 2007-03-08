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
package org.safehaus.penrose.engine.simple;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.engine.Engine;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class AddEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    Engine engine;

    public AddEngine(Engine engine) {
        this.engine = engine;
    }

    public void add(
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            DN dn,
            AttributeValues attributeValues)
            throws Exception {

        Collection sourceMappings = entryMapping.getSourceMappings();

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

            Map entries = engine.getTransformEngine().split(partition, entryMapping, sourceMapping, dn, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                RDN pk = (RDN)j.next();
                AttributeValues sv = (AttributeValues)entries.get(pk);

                if (log.isDebugEnabled()) log.debug("Adding to "+sourceMapping.getName()+" entry "+pk+": "+sv);

                engine.getConnector(sourceConfig).add(partition, sourceConfig, sv);
            }
        }
    }
}
