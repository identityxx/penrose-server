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
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import com.novell.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class LoadEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;
    private EngineContext engineContext;

    public LoadEngine(Engine engine) {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
    }

    public void load(
            Collection parentSourceValues,
            EntryDefinition entryDefinition,
            SearchResults batches,
            SearchResults loadedBatches
            ) throws Exception {

        //MRSWLock lock = getLock(entryDefinition.getDn());
        //lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            while (batches.hasNext()) {
                Collection keys = (Collection)batches.next();

                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("LOAD ("+entryDefinition.getDn()+")", 80));

                for (Iterator i=keys.iterator(); i.hasNext(); ) {
                    Map map = (Map)i.next();
                    String dn = (String)map.get("dn");
                    AttributeValues sv = (AttributeValues)map.get("sourceValues");
                    Row filter = (Row)map.get("filter");

                    log.debug(Formatter.displayLine(" - "+dn, 80));
                    log.debug(Formatter.displayLine("   filter: "+filter, 80));

                    for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                        String name = (String)j.next();
                        Collection values = sv.get(name);
                        log.debug(Formatter.displayLine("   - "+name+": "+values, 80));
                    }
                }

                log.debug(Formatter.displaySeparator(80));

                AttributeValues loadedSourceValues = loadEntries(parentSourceValues, entryDefinition, keys);

                for (Iterator i=keys.iterator(); i.hasNext(); ) {
                    Map map = (Map)i.next();

                    map.put("loadedSourceValues", loadedSourceValues);

                    loadedBatches.add(map);
                }
            }

        } finally {
            //lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            loadedBatches.close();
        }
    }

    public AttributeValues loadEntries(
            Collection parentSourceValues,
            EntryDefinition entryDefinition,
            Collection maps)
            throws Exception {

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=parentSourceValues.iterator(); i.hasNext(); ) {
            AttributeValues values = (AttributeValues)i.next();
            sourceValues.add(values);
        }

        Config config = engineContext.getConfig(entryDefinition.getDn());
        Graph graph = engine.getGraph(entryDefinition);
        Source primarySource = engine.getPrimarySource(entryDefinition);

        if (primarySource == null) return sourceValues;

        Collection pks = new TreeSet();
        for (Iterator i=maps.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();
            String dn = (String)m.get("dn");
            AttributeValues sv = (AttributeValues)m.get("sourceValues");
            Row pk = (Row)m.get("filter");
            pks.add(pk);
        }

        Filter filter  = engineContext.getFilterTool().createFilter(pks, true);

        Map map = new HashMap();
        map.put("attributeValues", sourceValues);
        map.put("filter", filter);

        Collection filters = new ArrayList();
        filters.add(map);

        LoadGraphVisitor loadVisitor = new LoadGraphVisitor(config, graph, engine, entryDefinition, parentSourceValues, filters, primarySource);
        graph.traverse(loadVisitor, primarySource);

        return loadVisitor.getLoadedSourceValues();
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
