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
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.interpreter.Interpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;
    private EngineContext engineContext;

    public SearchEngine(Engine engine) {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
    }

    public Map search(Entry parent, EntryDefinition entryDefinition, Filter filter) throws Exception {

        String dn = entryDefinition.getRdn()+","+parent.getDn();
        log.debug("Searching entry "+dn+" for "+filter);

        AttributeValues allValues = new AttributeValues();
        engine.getFieldValues("parent", parent, allValues);

        Collection newRows = engineContext.getTransformEngine().convert(allValues);

        Config config = engineContext.getConfig(entryDefinition.getDn());

        Graph graph = engine.getGraph(entryDefinition);
        Source primarySource = engine.getPrimarySource(entryDefinition);

        Collection rows = new TreeSet();

        String startingSourceName = engine.getStartingSourceName(entryDefinition);

        if (startingSourceName != null) {            
            Source startingSource = config.getEffectiveSource(entryDefinition, startingSourceName);
            log.debug("Starting from source "+startingSourceName);

            Object object = null;
            if (entryDefinition.getSource(startingSourceName) == null) {
                object = newRows;
            } else {
                object = engine.createFilter(startingSource, rows);
            }

            SearchGraphVisitor visitor = new SearchGraphVisitor(config, graph, engine, entryDefinition, object, primarySource);
            graph.traverse(visitor, startingSource);
            rows.addAll(visitor.getKeys());
        }

        return computeRdns(entryDefinition, rows);
    }

    public Map computeRdns(EntryDefinition entryDefinition, Collection rows) throws Exception {
        Source primarySource = engine.getPrimarySource(entryDefinition);

        log.debug("Search results:");
        Map rdns = new TreeMap();

        Collection rdnAttributes = entryDefinition.getRdnAttributes();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            Interpreter interpreter = engineContext.newInterpreter();
            for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                Object value = row.get(name);
                if (value == null) continue;
                interpreter.set(primarySource.getName()+"."+name, value);
            }

            Row rdn = new Row();
            boolean valid = true;

            for (Iterator j=rdnAttributes.iterator(); j.hasNext(); ) {
                AttributeDefinition attr = (AttributeDefinition)j.next();
                String name = attr.getName();

                Expression expression = attr.getExpression();
                if (expression == null) continue;

                Object value = interpreter.eval(expression);

                if (value == null) {
                    valid = false;
                    break;
                }

                rdn.set(name, value);
            }

            if (!valid) continue;

            Row nrdn = engineContext.getSchema().normalize(rdn);
            //log.debug(" - RDN: "+nrdn);

            rdns.put(nrdn, row);
        }

        return rdns;
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
