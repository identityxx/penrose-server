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
package org.safehaus.penrose.engine.impl;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.Attribute;
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

    EngineImpl engine;

    public ModRdnEngine(EngineImpl engine) {
        this.engine = engine;
    }

    public void modrdn(
            Partition partition,
            Entry entry,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        try {
            EntryMapping entryMapping = null; // entry.getEntryMapping();

            SourceValues oldAttributeValues = new SourceValues();
            for (Iterator i=entry.getAttributes().getAll().iterator(); i.hasNext(); ) {
                Attribute attribute = (Attribute)i.next();
                //oldAttributeValues.set(attribute.getName(), attribute.getValues());
            }
            
            SourceValues newAttributeValues = new SourceValues(oldAttributeValues);

            RDN rdn1 = entry.getDn().getRdn();
            //oldAttributeValues.set("rdn", rdn1);

            RDN rdn2 = newRdn;
            //newAttributeValues.set("rdn", rdn2);
            //newAttributeValues.add(rdn2);

            log.debug("Renaming "+rdn1+" to "+rdn2);

            if (deleteOldRdn) {
                log.debug("Removing old RDN:");
                for (Iterator i=rdn1.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Object value = rdn1.get(name);
                    //newAttributeValues.remove(name, value);
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
            SourceValues oldSv = null; // entry.getSourceValues();
            SourceValues newSv = new SourceValues(oldSv);
            Collection sources = entryMapping.getSourceMappings();
            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();

                SourceValues output = new SourceValues();
                engine.transformEngine.translate(
                        partition,
                        entryMapping,
                        sourceMapping,
                        entry.getDn(),
                        newAttributeValues,
                        output
                );

                //newSv.set(sourceMapping.getName(), output);
            }
/*
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
                for (Iterator iterator = oldSv.getNames().iterator(); iterator.hasNext(); ) {
                    String name = (String)iterator.next();
                    Collection values = oldSv.get(name);
                    log.debug(" - "+name+": "+values);
                }

                log.debug("New source values:");
                for (Iterator iterator = newSv.getNames().iterator(); iterator.hasNext(); ) {
                    String name = (String)iterator.next();
                    Collection values = newSv.get(name);
                    log.debug(" - "+name+": "+values);
                }
            }
*/
            ModRdnGraphVisitor visitor = new ModRdnGraphVisitor(engine, partition, entryMapping, oldSv, newSv);
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
