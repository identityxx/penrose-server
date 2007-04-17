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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.SourceValues;
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

    EngineImpl engine;

    public ModifyEngine(EngineImpl engine) {
        this.engine = engine;
    }

    public void modify(
            Partition partition,
            Entry entry,
            SourceValues newValues
    ) throws LDAPException {

        try {
            EntryMapping entryMapping = null; // entry.getEntryMapping();
            SourceValues oldSourceValues = null; // entry.getSourceValues();

            SourceValues newSourceValues = (SourceValues)oldSourceValues.clone();
            Collection sources = entryMapping.getSourceMappings();
            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();

                SourceValues output = new SourceValues();
                engine.transformEngine.translate(
                        partition,
                        entryMapping,
                        sourceMapping,
                        entry.getDn(),
                        newValues,
                        output
                );
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
