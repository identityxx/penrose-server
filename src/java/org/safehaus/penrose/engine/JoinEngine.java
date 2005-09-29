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
            final Entry parent,
            final EntryDefinition entryDefinition,
            final Collection rdnsToLoad)
            throws Exception {

        final SearchResults results = new SearchResults();

        engine.execute(new Runnable() {
            public void run() {
                try {
                    load(parent, entryDefinition, rdnsToLoad, results);

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
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdnsToLoad,
            SearchResults results)
            throws Exception {

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Graph graph = engine.getGraph(entryDefinition);
        Source primarySource = engine.getPrimarySource(entryDefinition);

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

        Collection filters = rdnToFilter(entryDefinition, rdnsToLoad);

        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(config, graph, engineContext, entryDefinition, filters);
        graph.traverse(loaderVisitor, primarySource);

        Map attributeValues = loaderVisitor.getAttributeValues();

        merge(parent, entryDefinition, attributeValues, results);
    }

    public Collection rdnToFilter(EntryDefinition entryDefinition, Collection rdns) throws Exception {

        Collection filters = new TreeSet();

        Source primarySource = engine.getPrimarySource(entryDefinition);
        Collection fields = primarySource.getFields();

        log.debug("Creating filters:");
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();
            log.debug(" - "+rdn);

            Interpreter interpreter = engineContext.newInterpreter();
            interpreter.set(rdn);

            Row filter = new Row();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();

                String name = field.getName();
                Expression expression = field.getExpression();
                if (expression == null) continue;

                String foreach = expression.getForeach();
                String var = expression.getVar();
                String script = expression.getScript();

                Object value = null;
                if (foreach == null) {
                    //log.debug("Evaluating expression: "+expression);
                    value = interpreter.eval(script);

                } else {
                    //log.debug("Evaluating expression: "+expression);

                    Object v = interpreter.get(foreach);
                    //log.debug("Values: "+v);

                    if (v != null) {
                        Collection values;
                        if (v instanceof Collection) {
                            values = (Collection)v;
                        } else {
                            values = new ArrayList();
                            values.add(v);
                        }

                        Collection newValues = new ArrayList();
                        for (Iterator k=values.iterator(); k.hasNext(); ) {
                            Object o = k.next();
                            interpreter.set(var, o);
                            value = interpreter.eval(script);
                            //log.debug(" - "+value);
                            newValues.add(value);
                        }

                        value = newValues;
                    }
                }

                //log.debug("   - "+primarySource.getName()+"."+field.getName()+": "+value);

                if (value == null) continue;

                filter.set(primarySource.getName()+"."+name, value);
            }

            if (filter.isEmpty()) continue;

            filters.add(filter);
        }

        log.debug("Filters: "+filters);

        return filters;
    }

    public void merge(Entry parent, EntryDefinition entryDefinition, Map pkValuesMap, SearchResults results) throws Exception {

        log.debug("Merging:");
        int counter = 1;

        // merge rows into attribute values
        for (Iterator i = pkValuesMap.keySet().iterator(); i.hasNext(); counter++) {
            Row pk = (Row)i.next();
            AttributeValues sourceValues = (AttributeValues)pkValuesMap.get(pk);

            log.debug(" - "+pk+": "+sourceValues);

            AttributeValues attributeValues = new AttributeValues();

            Row rdn = engineContext.getTransformEngine().translate(entryDefinition, sourceValues, attributeValues);
            if (rdn == null) continue;

            //log.debug("   => "+rdn+": "+attributeValues);

            Entry entry = new Entry(rdn+","+parent.getDn(), entryDefinition, sourceValues, attributeValues);
            entry.setParent(parent);
            results.add(entry);

            log.debug("Entry #"+counter+":\n"+entry+"\n");
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
