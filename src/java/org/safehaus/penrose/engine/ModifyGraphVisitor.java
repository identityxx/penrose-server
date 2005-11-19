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
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    public Engine engine;
    public EntryDefinition entryDefinition;

    public Graph graph;
    public Source primarySource;

    public AttributeValues oldSourceValues;
    public AttributeValues newSourceValues;

    private int returnCode = LDAPException.SUCCESS;

    public ModifyGraphVisitor(
            Engine engine,
            EntryDefinition entryDefinition,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues
            ) throws Exception {

        this.engine = engine;
        this.entryDefinition = entryDefinition;

        this.oldSourceValues = oldSourceValues;
        this.newSourceValues = newSourceValues;

        graph = engine.getGraph(entryDefinition);
        primarySource = engine.getPrimarySource(entryDefinition);
    }

    public void run() throws Exception {
        graph.traverse(this, primarySource);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        Source source = (Source)node;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(40));
            log.debug(Formatter.displayLine("Visiting "+source.getName(), 40));
            log.debug(Formatter.displaySeparator(40));
        }

        if (!source.isIncludeOnModify()) {
            log.debug("Source "+source.getName()+" is not included on modify");
            graphIterator.traverseEdges(node);
            return;
        }

        if (entryDefinition.getSource(source.getName()) == null) {
            log.debug("Source "+source.getName()+" is not defined in entry "+entryDefinition.getDn());
            graphIterator.traverseEdges(node);
            return;
        }

        log.debug("Old values:");
        AttributeValues oldValues = new AttributeValues();
        for (Iterator i=oldSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (!name.startsWith(source.getName()+".")) continue;

            Collection values = oldSourceValues.get(name);
            log.debug(" - "+name+": "+values);

            name = name.substring(source.getName().length()+1);
            oldValues.set(name, values);
        }

        log.debug("New values:");
        AttributeValues newValues = new AttributeValues();
        for (Iterator i=newSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (!name.startsWith(source.getName()+".")) continue;

            Collection values = newSourceValues.get(name);
            log.debug(" - "+name+": "+values);

            name = name.substring(source.getName().length()+1);
            newValues.set(name, values);
        }

        Config config = engine.getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        returnCode = engine.getConnector().modify(sourceDefinition, oldValues, newValues);
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
