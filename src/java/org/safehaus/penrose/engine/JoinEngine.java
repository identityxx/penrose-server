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
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import com.novell.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class JoinEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;
    private EngineContext engineContext;

    public JoinEngine(Engine engine) {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
    }

    public SearchResults load(
            final Collection parents,
            final EntryDefinition entryDefinition,
            final Collection pks)
            throws Exception {

        final SearchResults results = new SearchResults();

        engine.execute(new Runnable() {
            public void run() {
                try {
                    load(parents, entryDefinition, pks, results);

                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    results.close();
                }
            }
        });

        return results;
    }

    public void load(
            Collection parents,
            EntryDefinition entryDefinition,
            Collection pks,
            SearchResults results)
            throws Exception {

        Entry parent = parents.isEmpty() ? null : (Entry)parents.iterator().next();
        String dn = parent == null ? entryDefinition.getDn() : entryDefinition.getRdn()+","+parent.getDn();
        log.debug("Loading entry "+dn+" with pks "+pks);

        AttributeValues sourceValues = new AttributeValues();
        log.debug("Parent entries:");
        for (Iterator i=parents.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            AttributeValues values = entry.getSourceValues();
            log.debug(" - "+entry.getDn()+": "+values);
            sourceValues.add(values);
        }
        log.debug("Parent values: "+sourceValues);

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Graph graph = engine.getGraph(entryDefinition);
        Source primarySource = engine.getPrimarySource(entryDefinition);

        if (primarySource == null) {
            engine.createEntries(entryDefinition, sourceValues, results);
            results.close();
            return;
        }
/*
        ExecutionPlanner executionPlanner = new ExecutionPlanner(config, graph, entryDefinition);
        graph.traverse(executionPlanner, primarySource);

        Collection sources = executionPlanner.getSources();
        Map filterMap = executionPlanner.getFilters();

        log.debug("Execution plan:");
        for (Iterator i = sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            log.debug(" - load source "+source.getName());

            Collection filters = (Collection)filterMap.get(source);
            if (filters == null) continue;

            for (Iterator j=filters.iterator(); j.hasNext(); ) {
                Relationship filter = (Relationship)j.next();
                log.debug("   - filter with "+filter);
            }
        }
*/
        //Filter filter  = engineContext.getFilterTool().createFilter(pks);

        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(config, graph, engine, entryDefinition, sourceValues, pks);
        graph.traverse(loaderVisitor, primarySource);
        //Map attributeValues = loaderVisitor.getAttributeValues();

        Collection entries = new ArrayList();
        for (Iterator i=pks.iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            Collection list = new ArrayList();
            list.add(pk);

            //Filter filter  = engineContext.getFilterTool().createFilter(pk, true);

            MergerGraphVisitor mergerVisitor = new MergerGraphVisitor(config, graph, engine, entryDefinition, sourceValues, list);
            graph.traverse(mergerVisitor, primarySource);

            AttributeValues av = mergerVisitor.getAttributeValues();
            log.debug("Merged: "+av);

            entries.add(av);
        }

        merge(parent, entryDefinition, entries, results);
    }

    public void merge(Entry parent, EntryDefinition entryDefinition, Collection entries, SearchResults results) throws Exception {

        log.debug("Merging:");

        // merge rows into attribute values
        for (Iterator i = entries.iterator(); i.hasNext(); ) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            engine.createEntries(entryDefinition, sourceValues, results);
        }

        results.close();
    }


    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
    }
}
