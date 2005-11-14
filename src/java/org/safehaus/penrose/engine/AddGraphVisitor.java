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

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.graph.GraphVisitor;
import org.safehaus.penrose.graph.GraphIterator;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    public Engine engine;
    public EntryDefinition entryDefinition;
    public AttributeValues sourceValues;

    private int returnCode = LDAPException.SUCCESS;

    public AddGraphVisitor(
            Engine engine,
            EntryDefinition entryDefinition,
            AttributeValues sourceValues
            ) throws Exception {

        this.engine = engine;
        this.entryDefinition = entryDefinition;
        this.sourceValues = sourceValues;
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;

        log.debug(Formatter.displaySeparator(40));
        log.debug(Formatter.displayLine("Visiting "+source.getName(), 40));
        log.debug(Formatter.displaySeparator(40));

        if (!source.isIncludeOnAdd()) {
            log.debug("Source "+source.getName()+" is not included on add");
            graphIterator.traverseEdges(node);
            return;
        }

        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            graphIterator.traverseEdges(node);
            return;
        }

        log.debug("Adding values:");
        AttributeValues newSourceValues = new AttributeValues();
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (!name.startsWith(source.getName()+".")) continue;

            Collection values = sourceValues.get(name);
            log.debug(" - "+name+": "+values);

            name = name.substring(source.getName().length()+1);
            newSourceValues.set(name, values);
        }

        Config config = engine.getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        returnCode = engine.getConnector().add(sourceDefinition, newSourceValues);
        if (returnCode != LDAPException.SUCCESS) return;

        graphIterator.traverseEdges(node);
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }
}
