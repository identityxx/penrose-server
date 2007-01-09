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
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.partition.Partition;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;

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
            String dn,
            AttributeValues attributeValues
    ) throws Exception {

        try {
            Row rdn = EntryUtil.getRdn(dn);
            attributeValues.set("rdn", rdn);

            AttributeValues parentSourceValues = parent.getSourceValues();
            AttributeValues sourceValues = new AttributeValues();

            Collection sources = entryMapping.getSourceMappings();
            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();

                AttributeValues output = new AttributeValues();
                Row pk = engine.getTransformEngine().translate(partition, entryMapping, sourceMapping, attributeValues, output);
                if (pk == null) continue;

                sourceValues.set(sourceMapping.getName()+".primaryKey", pk);

                for (Iterator j=output.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = output.get(name);
                    sourceValues.add(sourceMapping.getName()+"."+name, values);
                }
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));

                log.debug(Formatter.displayLine("Parent source values:", 80));
                for (Iterator i = parentSourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = parentSourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }

                log.debug(Formatter.displayLine("Local source values:", 80));
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }

                log.debug(Formatter.displaySeparator(80));
            }

            sourceValues.add(parentSourceValues);

            Graph graph = engine.getGraph(entryMapping);
            String startingSourceName = engine.getStartingSourceName(partition, entryMapping);
            if (startingSourceName == null) return;

            SourceMapping startingSourceMapping = partition.getEffectiveSourceMapping(entryMapping, startingSourceName);
            log.debug("Starting from source: "+startingSourceName);

            Collection relationships = graph.getEdgeObjects(startingSourceMapping);
            for (Iterator i=relationships.iterator(); i.hasNext(); ) {
                Collection list = (Collection)i.next();

                for (Iterator j=list.iterator(); j.hasNext(); ) {
                    Relationship relationship = (Relationship)j.next();
                    log.debug("Relationship "+relationship);

                    String lhs = relationship.getLhs();
                    String rhs = relationship.getRhs();

                    if (rhs.startsWith(startingSourceName+".")) {
                        String exp = lhs;
                        lhs = rhs;
                        rhs = exp;
                    }

                    Collection lhsValues = sourceValues.get(lhs);
                    log.debug(" - "+lhs+" -> "+rhs+": "+lhsValues);
                    sourceValues.set(rhs, lhsValues);
                }
            }

            AddGraphVisitor visitor = new AddGraphVisitor(engine, partition, entryMapping, sourceValues);
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
