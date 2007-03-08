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
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.Engine;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.ModificationItem;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ModifyEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    Engine engine;

    public ModifyEngine(Engine engine) {
        this.engine = engine;
    }

    public void modify(
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            Collection modifications,
            AttributeValues newValues
    ) throws Exception {

        RDN rdn = dn.getRdn();
        AttributeValues attributeValues = new AttributeValues();
        attributeValues.add(rdn);

        for (Iterator iterator=modifications.iterator(); iterator.hasNext(); ) {
            ModificationItem mi = (ModificationItem)iterator.next();
            Attribute attribute = mi.getAttribute();
            String name = attribute.getID();

            for (NamingEnumeration ne = attribute.getAll(); ne.hasMore(); ) {
                attributeValues.add(name, ne.next());
            }
        }

        Collection sourceMappings = entryMapping.getSourceMappings();
        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);
        if (log.isDebugEnabled()) log.debug("Primary source: "+primarySourceMapping.getName());
        
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            if (log.isDebugEnabled()) log.debug("Modifying source "+sourceMapping.getName());

            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

            Map entries = engine.getTransformEngine().split(partition, entryMapping, sourceMapping, dn, attributeValues);

            RDN pk = (RDN)entries.keySet().iterator().next();
            RDNBuilder rb = new RDNBuilder();
            rb.set(pk);

            boolean deleteExistingEntries = false;
            // Convert modification list of attributes into modification list of fields
            Collection mods = new ArrayList();
            for (Iterator j=sourceMapping.getFieldMappings().iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                if (fieldMapping.getVariable() == null) continue;

                String name = fieldMapping.getName();
                String variable = fieldMapping.getVariable();

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);

                for (Iterator k=modifications.iterator(); k.hasNext(); ) {
                    ModificationItem mi = (ModificationItem)k.next();
                    Attribute attribute = mi.getAttribute();
                    if (!attribute.getID().equals(variable)) continue;

                    if (log.isDebugEnabled()) log.debug("Converting modification for attribute "+variable);
                    int op = mi.getModificationOp();
                    Attribute newAttr = new BasicAttribute(name);
                    for (NamingEnumeration ne = attribute.getAll(); ne.hasMore(); ) {
                        newAttr.add(ne.next());
                    }
                    mods.add(new ModificationItem(op, newAttr));

                    if ((!sourceMapping.equals(primarySourceMapping)) && fieldConfig.isPrimaryKey()) {
                        deleteExistingEntries = true;
                        if (log.isDebugEnabled()) log.debug("Removing field "+name);
                        rb.remove(name);
                    }
                }
            }

            RDN pk2 = rb.toRdn();

            if (log.isDebugEnabled()) log.debug("PK: "+pk);
            if (log.isDebugEnabled()) log.debug("PK2: "+pk2);

            Connector connector = engine.getConnector(sourceConfig);
            if (deleteExistingEntries) {
                Connection connection = connector.getConnection(partition, sourceConfig.getConnectionName());
                connection.delete(sourceConfig, pk2);
            }

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                pk = (RDN)j.next();
                AttributeValues sv = (AttributeValues)entries.get(pk);
                if (log.isDebugEnabled()) log.debug("Modifying entry "+pk+" in "+sourceConfig.getName()+": "+sv);

                connector.modify(partition, sourceConfig, pk, mods, sv, newValues);
            }
        }
    }
}
