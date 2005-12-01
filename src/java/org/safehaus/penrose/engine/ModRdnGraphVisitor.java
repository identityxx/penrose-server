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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModRdnGraphVisitor extends GraphVisitor {

    Logger log = Logger.getLogger(getClass());

    public Engine engine;
    public EntryMapping entryMapping;

    public Graph graph;
    public SourceMapping primarySourceMapping;

    public AttributeValues oldSourceValues;
    public AttributeValues newSourceValues;
    private AttributeValues modifiedSourceValues = new AttributeValues();

    private int returnCode = LDAPException.SUCCESS;

    public ModRdnGraphVisitor(
            Engine engine,
            EntryMapping entryMapping,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues
            ) throws Exception {

        this.engine = engine;
        this.entryMapping = entryMapping;

        this.oldSourceValues = oldSourceValues;
        this.newSourceValues = newSourceValues;

        modifiedSourceValues.add(newSourceValues);

        graph = engine.getGraph(entryMapping);
        primarySourceMapping = engine.getPrimarySource(entryMapping);
    }

    public void run() throws Exception {
        graph.traverse(this, primarySourceMapping);
    }

    public void visitNode(GraphIterator graphIterator, Object node) throws Exception {

        SourceMapping sourceMapping = (SourceMapping)node;

        log.debug(Formatter.displaySeparator(40));
        log.debug(Formatter.displayLine("Visiting "+sourceMapping.getName(), 40));
        log.debug(Formatter.displaySeparator(40));

        if (sourceMapping.isReadOnly() || !sourceMapping.isIncludeOnModRdn()) {
            log.debug("Source "+sourceMapping.getName()+" is not included on modify");
            graphIterator.traverseEdges(node);
            return;
        }

        if (entryMapping.getSourceMapping(sourceMapping.getName()) == null) {
            log.debug("Source "+sourceMapping.getName()+" is not defined in entry "+entryMapping.getDn());
            graphIterator.traverseEdges(node);
            return;
        }

        log.debug("Old values:");
        AttributeValues oldValues = new AttributeValues();
        for (Iterator i=oldSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (!name.startsWith(sourceMapping.getName()+".")) continue;

            Collection values = oldSourceValues.get(name);
            log.debug(" - "+name+": "+values);

            name = name.substring(sourceMapping.getName().length()+1);
            oldValues.set(name, values);
        }

        log.debug("New values:");
        AttributeValues newValues = new AttributeValues(oldValues);
        for (Iterator i=newSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (!name.startsWith(sourceMapping.getName()+".")) continue;

            Collection values = newSourceValues.get(name);
            log.debug(" - "+name+": "+values);

            name = name.substring(sourceMapping.getName().length()+1);
            newValues.set(name, values);
        }

        Partition partition = engine.getPartitionManager().getPartition(sourceMapping);
        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        returnCode = engine.getConnector().modify(sourceConfig, oldValues, newValues);
        if (returnCode != LDAPException.SUCCESS) return;

        modifiedSourceValues.remove(sourceMapping.getName());
        modifiedSourceValues.set(sourceMapping.getName(), newValues);

        graphIterator.traverseEdges(node);
    }

    public void visitEdge(GraphIterator graphIterator, Object node1, Object node2, Object object) throws Exception {

        SourceMapping fromSourceMapping = (SourceMapping)node1;
        SourceMapping toSourceMapping = (SourceMapping)node2;
        Collection relationships = (Collection)object;

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(60));
            for (Iterator i=relationships.iterator(); i.hasNext(); ) {
                Relationship relationship = (Relationship)i.next();
                log.debug(Formatter.displayLine(relationship.toString(), 60));
            }
            log.debug(Formatter.displaySeparator(60));
        }

        if (entryMapping.getSourceMapping(toSourceMapping.getName()) == null) {
            log.debug("Source "+toSourceMapping.getName()+" is not defined in entry.");
            return;
        }


        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String lhs = relationship.getLhs();
            String rhs = relationship.getRhs();

            if (lhs.startsWith(toSourceMapping.getName())) {
                String tmp = lhs;
                lhs = rhs;
                rhs = tmp;
            }

            Collection values = newSourceValues.get(lhs);
            newSourceValues.set(rhs, values);

            log.debug(lhs+" ==> "+rhs+": "+values);
        }

        graphIterator.traverse(node2);
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public AttributeValues getModifiedSourceValues() {
        return modifiedSourceValues;
    }

    public void setModifiedSourceValues(AttributeValues modifiedSourceValues) {
        this.modifiedSourceValues = modifiedSourceValues;
    }
}
