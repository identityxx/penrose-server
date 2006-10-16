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
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.Partition;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModifyEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    Engine engine;

    public ModifyEngine(Engine engine) {
        this.engine = engine;
    }

    public int modify(Partition partition, Entry entry, AttributeValues newValues) throws Exception {

        EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues oldSourceValues = entry.getSourceValues();

        AttributeValues newSourceValues = (AttributeValues)oldSourceValues.clone();
        Collection sources = entryMapping.getSourceMappings();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            AttributeValues output = new AttributeValues();
            engine.getTransformEngine().translate(partition, entryMapping, sourceMapping, newValues, output);
            newSourceValues.set(sourceMapping.getName(), output);
        }

        if (log.isDebugEnabled()) {
            log.debug("Old source values:");
            for (Iterator iterator = oldSourceValues.getNames().iterator(); iterator.hasNext(); ) {
                String name = (String)iterator.next();
                Collection values = oldSourceValues.get(name);
                log.debug(" - "+name+": "+values);
            }

            log.debug("New source values:");
            for (Iterator iterator = newSourceValues.getNames().iterator(); iterator.hasNext(); ) {
                String name = (String)iterator.next();
                Collection values = newSourceValues.get(name);
                log.debug(" - "+name+": "+values);
            }
        }

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(engine, partition, entryMapping, oldSourceValues, newSourceValues);
        visitor.run();

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        return LDAPException.SUCCESS;
    }
}
