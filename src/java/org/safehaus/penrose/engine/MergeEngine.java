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
public class MergeEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;
    private EngineContext engineContext;

    public MergeEngine(Engine engine) {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
    }

    public void merge(
            EntryDefinition entryDefinition,
            SearchResults loadedBatches,
            SearchResults results
            ) throws Exception {

        //MRSWLock lock = getLock(entryDefinition.getDn());
        //lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            while (loadedBatches.hasNext()) {
                Map map = (Map)loadedBatches.next();

                String dn = (String)map.get("dn");
                AttributeValues primarySourceValues = (AttributeValues)map.get("sourceValues");
                Row filter = (Row)map.get("filter");
                AttributeValues loadedSourceValues = (AttributeValues)map.get("loadedSourceValues");

                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("MERGE", 80));
                log.debug(Formatter.displayLine(" - "+dn, 80));

                for (Iterator j=primarySourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection v = primarySourceValues.get(name);
                    log.debug(Formatter.displayLine("   "+name+": "+v, 80));
                }

                log.debug(Formatter.displaySeparator(80));

                mergeEntries(dn, entryDefinition, primarySourceValues, loadedSourceValues, results);
            }

        } finally {
            //lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }
    }

    public SearchResults mergeEntries(
            String dn,
            EntryDefinition entryDefinition,
            AttributeValues primarySourceValues,
            AttributeValues loadedSourceValues,
            SearchResults results)
            throws Exception {

        AttributeValues sourceValues;
        Source primarySource = engine.getPrimarySource(entryDefinition);

        if (primarySource != null) {

            MergeGraphVisitor merger = new MergeGraphVisitor(
                    engine,
                    entryDefinition,
                    primarySourceValues,
                    loadedSourceValues,
                    primarySource
            );

            merger.run();

            sourceValues = merger.getSourceValues();
/*
            log.debug("Merged source values:");

            Collection values = merger.getResults();
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                AttributeValues av = (AttributeValues)j.next();
                log.debug(" - "+av);
                sourceValues.add(av);
            }
*/
        } else {
            sourceValues = new AttributeValues();
        }

        AttributeValues attributeValues = engine.computeAttributeValues(entryDefinition, sourceValues);
        Entry entry = new Entry(dn, entryDefinition, sourceValues, attributeValues);

        engineContext.getEntryDataCache(entry.getParentDn(), entryDefinition).put(entry.getRdn(), entry);

        log.debug("Entry:");
        log.debug(" - source values: "+entry.getSourceValues());
        log.debug(" - attribute values: "+entry.getAttributeValues());
        log.debug("\n"+entry);

        results.add(entry);

        return results;
    }

    public void mergeEntries(
            String dn,
            EntryDefinition entryDefinition,
            AttributeValues sourceValues,
            Map entries)
            throws Exception {

        AttributeValues attributeValues = engine.computeAttributeValues(entryDefinition, sourceValues);
        //Collection dns = engine.computeDns(entryDefinition, sourceValues);

        //for (Iterator i = dns.iterator(); i.hasNext(); ) {
            //String dn = (String)i.next();
            //log.debug("Merging entry "+dn);

            Entry entry = (Entry)entries.get(dn);
            if (entry == null) {
                entry = new Entry(dn, entryDefinition, sourceValues, attributeValues);
                entries.put(dn, entry);
                //continue;
            }
/*
            AttributeValues sv = entry.getSourceValues();
            sv.add(sourceValues);

            AttributeValues av = entry.getAttributeValues();
            av.add(attributeValues);
*/
        //}
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
