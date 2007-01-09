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
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.partition.Partition;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;

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
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        try {
            EntryMapping entryMapping = entry.getEntryMapping();

            AttributeValues oldAttributeValues = entry.getAttributeValues();
            AttributeValues newAttributeValues = new AttributeValues(oldAttributeValues);

            Row rdn1 = EntryUtil.getRdn(entry.getDn());
            oldAttributeValues.set("rdn", rdn1);

            Row rdn2 = EntryUtil.getRdn(newRdn);
            newAttributeValues.set("rdn", rdn2);
            newAttributeValues.add(rdn2);

            log.debug("Renaming "+rdn1+" to "+rdn2);

            if (deleteOldRdn) {
                log.debug("Removing old RDN:");
                for (Iterator i=rdn1.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Object value = rdn1.get(name);
                    newAttributeValues.remove(name, value);
                    log.debug(" - "+name+": "+value);
                }
            }
/*
            Collection rdnAttributes = entryMapping.getRdnAttributes();
            for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)i.next();
                String name = attributeMapping.getName();
                String newValue = (String)rdn2.get(name);

                newAttributeValues.remove(name);
                newAttributeValues.add(name, newValue);
            }
*/
            AttributeValues oldSourceValues = entry.getSourceValues();
            AttributeValues newSourceValues = new AttributeValues(oldSourceValues);
            Collection sources = entryMapping.getSourceMappings();
            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();

                AttributeValues output = new AttributeValues();
                engine.getTransformEngine().translate(partition, entryMapping, sourceMapping, newAttributeValues, output);
                newSourceValues.set(sourceMapping.getName(), output);
            }

            if (log.isDebugEnabled()) {
                log.debug("Old attribute values:");
                for (Iterator iterator = oldAttributeValues.getNames().iterator(); iterator.hasNext(); ) {
                    String name = (String)iterator.next();
                    Collection values = newAttributeValues.get(name);
                    log.debug(" - "+name+": "+values);
                }

                log.debug("New attribute values:");
                for (Iterator iterator = newAttributeValues.getNames().iterator(); iterator.hasNext(); ) {
                    String name = (String)iterator.next();
                    Collection values = newAttributeValues.get(name);
                    log.debug(" - "+name+": "+values);
                }

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

            ModRdnGraphVisitor visitor = new ModRdnGraphVisitor(engine, partition, entryMapping, oldSourceValues, newSourceValues);
            visitor.run();

        } catch (LDAPException e) {
            throw e;

        } catch (Exception e) {
            int rc = ExceptionUtil.getReturnCode(e);
            String message = e.getMessage();
            log.error(message, e);
            throw new LDAPException(LDAPException.resultCodeToString(rc), rc, message);
        }
    }
}
