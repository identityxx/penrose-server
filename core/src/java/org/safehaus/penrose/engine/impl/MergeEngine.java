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
package org.safehaus.penrose.engine.impl;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EntryData;
import org.safehaus.penrose.entry.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class MergeEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private EngineImpl engine;

    public MergeEngine(EngineImpl engine) {
        this.engine = engine;
    }

    public void merge(
            final Partition partition,
            final EntryMapping entryMapping,
            final SearchResponse entries,
            final SearchResponse response
    ) throws Exception {

        final Interpreter interpreter = engine.getInterpreterManager().newInstance();

        try {
            while (entries.hasNext()) {
                EntryData map = (EntryData)entries.next();

                DN dn = map.getDn();
                AttributeValues primarySourceValues = map.getMergedValues();
                Collection rows = map.getRows();
                RDN filter = map.getFilter();
                AttributeValues loadedSourceValues = map.getLoadedSourceValues();

                Entry entry = mergeEntries(
                        partition,
                        dn,
                        entryMapping,
                        primarySourceValues,
                        loadedSourceValues,
                        rows,
                        interpreter,
                        filter
                );

                response.add(entry);
            }

        } finally {
            int rc = entries.getReturnCode();
            //log.debug("RC: "+rc);

            response.close();
        }
    }

    public Entry mergeEntries(
            Partition partition,
            DN dn,
            EntryMapping entryMapping,
            AttributeValues primarySourceValues,
            AttributeValues loadedSourceValues,
            Collection rows,
            Interpreter interpreter,
            RDN pk)
            throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MERGE", 80));
            log.debug(Formatter.displayLine("DN: "+dn, 80));
            log.debug(Formatter.displayLine("PK: "+pk, 80));

            if (primarySourceValues != null) {
                log.debug(Formatter.displayLine("Primary source values:", 80));
                for (Iterator j=primarySourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection v = primarySourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+v, 80));
                }
            }

            if (loadedSourceValues != null) {
                for (Iterator i=loadedSourceValues.getNames().iterator(); i.hasNext(); ) {
                    String sourceName = (String)i.next();
                    log.debug(Formatter.displayLine("Source "+sourceName+":", 80));
                    Collection avs = loadedSourceValues.get(sourceName);
                    for (Iterator j=avs.iterator(); j.hasNext(); ) {
                        AttributeValues av = (AttributeValues)j.next();

                        for (Iterator k=av.getNames().iterator(); k.hasNext(); ) {
                            String name = (String)k.next();
                            Collection values = av.get(name);
                            log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                        }
                    }
               }
            }

            log.debug(Formatter.displayLine("Rows:", 80));
            if (rows != null) {
                int counter = 0;
                for (Iterator j=rows.iterator(); j.hasNext() && counter <= 20; counter++) {
                    AttributeValues rdn = (AttributeValues)j.next();
                    log.debug(Formatter.displayLine(" - "+rdn, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        AttributeValues sourceValues;
        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        if (primarySourceMapping != null && loadedSourceValues != null) {

            RDNBuilder rb = new RDNBuilder();
            rb.add(primarySourceMapping.getName(), pk);
            RDN key = rb.toRdn();

            Filter filter  = FilterTool.createFilter(key, true);

            MergeGraphVisitor merger = new MergeGraphVisitor(
                    engine,
                    partition,
                    entryMapping,
                    primarySourceValues,
                    loadedSourceValues,
                    primarySourceMapping,
                    filter
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
            sourceValues = primarySourceValues;
        }

        AttributeValues attributeValues = engine.computeAttributeValues(
                entryMapping,
                sourceValues,
                //rows,
                interpreter
        );

        if (attributeValues == null) {
            log.debug("Attribute values: "+attributeValues);
            return null;
        }

        //log.debug(" - attribute values: "+attributeValues);

        log.debug("Generating entry "+dn);

        Entry entry = new Entry(dn, entryMapping, attributeValues, sourceValues);
        log.debug("\n"+EntryUtil.toString(entry));

        //RDN rdn = entry.getRdn();

        //log.debug("Storing "+rdn+" in entry data cache for "+entry.getParentDn());
        //engine.getEntryCache().put(entry);

        return entry;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(EngineImpl engine) {
        this.engine = engine;
    }
}
