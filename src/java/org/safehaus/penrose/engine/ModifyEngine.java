/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import org.apache.log4j.Logger;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.interpreter.Interpreter;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModifyEngine {

    Logger log = Logger.getLogger(getClass());

    Engine engine;

    public ModifyEngine(Engine engine) {
        this.engine = engine;
    }

    public int modify(Entry entry, AttributeValues newValues) throws Exception {

        EntryDefinition entryDefinition = entry.getEntryDefinition();
        AttributeValues oldSourceValues = entry.getSourceValues();

        AttributeValues newSourceValues = (AttributeValues)oldSourceValues.clone();
        Collection sources = entryDefinition.getSources();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            AttributeValues output = new AttributeValues();
            engine.getTransformEngine().translate(source, newValues, output);
            newSourceValues.set(source.getName(), output);
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

        ModifyGraphVisitor visitor = new ModifyGraphVisitor(engine, entryDefinition, oldSourceValues, newSourceValues);
        visitor.run();

        if (visitor.getReturnCode() != LDAPException.SUCCESS) return visitor.getReturnCode();

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();

        AttributeValues sourceValues = visitor.getModifiedSourceValues();
        AttributeValues attributeValues = engine.computeAttributeValues(entryDefinition, sourceValues, interpreter);
        Row rdn = entryDefinition.getRdn(attributeValues);
        String dn = rdn+","+entry.getParentDn();

        Entry newEntry = new Entry(dn, entryDefinition, sourceValues, attributeValues);

        engine.getCache(entry.getParentDn(), entryDefinition).remove(entry.getRdn());
        engine.getCache(entry.getParentDn(), entryDefinition).put(rdn, newEntry);

        return LDAPException.SUCCESS;
    }
}
