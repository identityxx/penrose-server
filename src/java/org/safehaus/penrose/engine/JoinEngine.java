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
import org.safehaus.penrose.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

    public Collection load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdnsToLoad)
            throws Exception {

        //AttributeValues sourceValues = new AttributeValues();
        //engine.getFieldValues("parent", parent, sourceValues);

        //log.debug("Loading entry "+entryDefinition.getRdn()+","+parent.getDn()+" with values: "+sourceValues);

        //Config config = engineContext.getConfig(entryDefinition.getDn());

        Graph graph = engine.getGraph(entryDefinition);
        Source primarySource = engine.getPrimarySource(entryDefinition);
/*
        String startingSourceName = engine.getStartingSourceName(entryDefinition);

        log.debug("Starting from source: "+startingSourceName);
        Source startingSource = config.getEffectiveSource(entryDefinition, startingSourceName);

        Collection relationships = graph.getEdgeObjects(startingSource);
        AttributeValues startingValues = new AttributeValues();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
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
            startingValues.set(rhs, lhsValues);
        }

        Collection values = engineContext.getTransformEngine().convert(startingValues);
*/
        Collection filters = rdnToFilter(entryDefinition, rdnsToLoad);
/*
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Row filter = (Row)i.next();
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();
                filter.add(row);
            }
        }
*/
        LoaderGraphVisitor loaderVisitor = new LoaderGraphVisitor(engineContext, entryDefinition, filters);
        graph.traverse(loaderVisitor, primarySource);

        Map attributeValues = loaderVisitor.getAttributeValues();

        return merge(parent, entryDefinition, attributeValues);
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
                Expression exp = field.getExpression();
                if (exp == null) continue;

                String script = exp.getScript();
                //log.debug("   - "+primarySource.getName()+"."+field.getName()+": "+script);

                Object value = interpreter.eval(script);
                if (value == null) continue;

                filter.set(primarySource.getName()+"."+field.getName(), value);
            }

            if (filter.isEmpty()) continue;

            filters.add(filter);
        }

        log.debug("Filters: "+filters);

        return filters;
    }

    public Collection merge(Entry parent, EntryDefinition entryDefinition, Map pkValuesMap) throws Exception {

        Collection results = new ArrayList();

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

        return results;
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
