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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.util.ExceptionUtil;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Handler handler;

    public SearchHandler(Handler handler) {
        this.handler = handler;
    }

    public void search(
            final PenroseSession session,
            final Partition partition,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Searching "+baseDn);
            log.debug("in mapping "+entryMapping.getDn());
        }

        AttributeValues sourceValues = new AttributeValues();

        final FilterTool filterTools = handler.getFilterTool();
        Pipeline sr = new Pipeline(results) {

            // Check LDAP filter
            public void add(Object object) throws Exception {
                Entry child = (Entry)object;

                if (debug) log.debug("Checking filter "+filter+" on "+child.getDn());

                if (!filterTools.isValid(child, filter)) {
                    if (debug) log.debug("Entry \""+child.getDn()+"\" doesn't match search filter.");
                    return;
                }

                super.add(child);
            }
        };

        if (sc.getScope() == LDAPConnection.SCOPE_BASE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // base or subtree
            searchBase(
                    session,
                    partition,
                    sourceValues,
                    entryMapping,
                    baseDn,
                    filter,
                    sc,
                    sr
            );
        }

        if (sc.getScope() == LDAPConnection.SCOPE_ONE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // one level or subtree

            Collection children = partition.getChildren(entryMapping);

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();

                searchChildren(
                        session,
                        partition,
                        sourceValues,
                        entryMapping,
                        childMapping,
                        baseDn,
                        filter,
                        sc,
                        sr
                );
            }
        }
	}

    /**
     * @param session
     * @param partition
     * @param sourceValues
     * @param entryMapping
     * @param baseDn
     * @param filter
     * @param sc
     * @param results Collection of entries (Entry).
     * @throws Exception
     */
    public void searchBase(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        String engineName = entryMapping.getEngineName();
        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            if (debug) log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        Pipeline sr = new Pipeline(results);

        engine.search(
                session,
                partition,
                sourceValues,
                entryMapping,
                baseDn,
                filter,
                sc,
                sr
        );
    }

    /**
     * @param session
     * @param partition
     * @param sourceValues
     * @param baseMapping
     * @param entryMapping
     * @param baseDn
     * @param filter
     * @param sc
     * @param results Collection of entries (Entry).
     * @throws Exception
     */
    public void searchChildren(
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Search mapping \""+entryMapping.getDn()+"\":");
        }

        String engineName = entryMapping.getEngineName();
        Engine engine = handler.getEngine(engineName);

        if (engine == null) {
            if (debug) log.debug("Engine "+engineName+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.OPERATIONS_ERROR);
        }

        // use a new pipeline so that results is not closed
        Pipeline sr = new Pipeline(results);

        engine.expand(
                session,
                partition,
                sourceValues,
                baseMapping,
                entryMapping,
                baseDn,
                filter,
                sc,
                sr
        );

        if (sc.getScope() != LDAPConnection.SCOPE_SUB) return;

        Collection children = partition.getChildren(entryMapping);
        if (children.isEmpty()) return;

        Interpreter interpreter = handler.getEngine().getInterpreterManager().newInstance();
        AttributeValues attributeValues = handler.getEngine().computeAttributeValues(entryMapping, sourceValues, interpreter);
        interpreter.clear();

        AttributeValues newSourceValues = new AttributeValues();
        newSourceValues.add("parent", sourceValues);
        newSourceValues.add("parent", attributeValues);
        //AttributeValues newSourceValues = handler.pushSourceValues(sourceValues, attributeValues);

        if (debug) {
            log.debug("New parent source values:");
            newSourceValues.print();
        }

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            searchChildren(
                    session,
                    partition,
                    newSourceValues,
                    baseMapping,
                    childMapping,
                    baseDn,
                    filter,
                    sc,
                    results
            );
        }
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
