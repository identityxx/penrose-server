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
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.Engine;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class ModRdnEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    Engine engine;

    public ModRdnEngine(Engine engine) {
        this.engine = engine;
    }

    public void modrdn(
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        RDN rdn1 = dn.getRdn();
        AttributeValues av1 = new AttributeValues();
        av1.add(rdn1);

        RDN rdn2 = newRdn;
        AttributeValues av2 = new AttributeValues();
        av2.add(rdn2);

        Collection sourceMappings = entryMapping.getSourceMappings();
        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);
        log.debug("Primary source: "+primarySourceMapping.getName());

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            log.debug("Renaming source "+sourceMapping.getName());
            
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

            Map entries1 = engine.getTransformEngine().split(partition, entryMapping, sourceMapping, dn, av1);
            Map entries2 = engine.getTransformEngine().split(partition, entryMapping, sourceMapping, dn, av2);

            log.debug("Entries 1: "+entries1);
            log.debug("Entries 2: "+entries2);

            RDN oldPk = (RDN)entries1.keySet().iterator().next();
            RDN newPk = (RDN)entries2.keySet().iterator().next();

            log.debug("Renaming "+newPk+" into "+newPk);

            Connector connector = engine.getConnector(sourceConfig);
            connector.modrdn(partition, sourceConfig, oldPk, newPk, deleteOldRdn);
        }
    }
}
