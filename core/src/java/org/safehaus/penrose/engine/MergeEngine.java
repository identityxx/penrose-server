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
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class MergeEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;

    public MergeEngine(Engine engine) {
        this.engine = engine;
    }

    public void merge(
            final Partition partition,
            final EntryMapping entryMapping,
            final PenroseSearchResults loadedBatches,
            final PenroseSearchResults results
            ) throws Exception {

        final Interpreter interpreter = engine.getInterpreterManager().newInstance();

        try {
            mergeBackground(partition, entryMapping, loadedBatches, interpreter, results);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(org.ietf.ldap.LDAPException.OPERATIONS_ERROR);
        }
    }

    public void mergeBackground(
            Partition partition,
            EntryMapping entryMapping,
            PenroseSearchResults entries,
            Interpreter interpreter,
            PenroseSearchResults results
            ) throws Exception {

        //MRSWLock lock = getLock(entryMapping;
        //lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            while (entries.hasNext()) {
                EntryData map = (EntryData)entries.next();

                String dn = map.getDn();
                AttributeValues primarySourceValues = map.getMergedValues();
                Collection rows = map.getRows();
                Row filter = map.getFilter();
                AttributeValues loadedSourceValues = map.getLoadedSourceValues();

                if (log.isDebugEnabled()) {
                    log.debug(Formatter.displaySeparator(80));
                    log.debug(Formatter.displayLine("MERGE", 80));
                    log.debug(Formatter.displayLine("Entry: "+dn, 80));
                    log.debug(Formatter.displayLine("Filter: "+filter, 80));

                    if (primarySourceValues != null) {
                        log.debug(Formatter.displayLine("Primary source values:", 80));
                        for (Iterator j=primarySourceValues.getNames().iterator(); j.hasNext(); ) {
                            String name = (String)j.next();
                            Collection v = primarySourceValues.get(name);
                            log.debug(Formatter.displayLine(" - "+name+": "+v, 80));
                        }
                    }

                    log.debug(Formatter.displayLine("Loaded source values:", 80));
                    if (loadedSourceValues != null) {
                        for (Iterator i=loadedSourceValues.getNames().iterator(); i.hasNext(); ) {
                            String sourceName = (String)i.next();
                            log.debug(Formatter.displayLine(" - "+sourceName+":", 80));
                            Collection avs = loadedSourceValues.get(sourceName);
                            for (Iterator j=avs.iterator(); j.hasNext(); ) {
                                AttributeValues av = (AttributeValues)j.next();
                                for (Iterator k=av.getNames().iterator(); k.hasNext(); ) {
                                    String name = (String)k.next();
                                    Collection values = av.get(name);
                                    log.debug(Formatter.displayLine("   - "+name+": "+values, 80));
                                }
                            }
                       }
                    }

                    log.debug(Formatter.displayLine("Rows:", 80));
                    if (rows != null) {
                        int counter = 0;
                        for (Iterator j=rows.iterator(); j.hasNext() && counter <= 20; counter++) {
                            AttributeValues row = (AttributeValues)j.next();
                            log.debug(Formatter.displayLine(" - "+row, 80));
                        }
                    }

                    log.debug(Formatter.displaySeparator(80));
                }

                mergeEntries(partition, dn, entryMapping, primarySourceValues, loadedSourceValues, rows, interpreter, filter, results);
            }

        } finally {
            //lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);

            int rc = entries.getReturnCode();
            //log.debug("RC: "+rc);

            results.setReturnCode(rc);
            results.close();
        }
    }

    public PenroseSearchResults mergeEntries(
            Partition partition,
            String dn,
            EntryMapping entryMapping,
            AttributeValues primarySourceValues,
            AttributeValues loadedSourceValues,
            Collection rows,
            Interpreter interpreter,
            Row pk,
            PenroseSearchResults results)
            throws Exception {

        AttributeValues sourceValues;
        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        if (primarySourceMapping != null && loadedSourceValues != null) {

            Row key = new Row();
            key.add(primarySourceMapping.getName(), pk);

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

        log.debug("Generating entry "+dn);

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            for (Iterator j=sourceValues.get(name).iterator(); j.hasNext(); ) {
                Object value = j.next();
                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".")+1);
                log.debug(" - "+name+" ("+className+"): "+value);
            }
        }

        AttributeValues attributeValues = engine.computeAttributeValues(
                entryMapping,
                sourceValues,
                rows,
                interpreter
        );

        //log.debug(" - attribute values: "+attributeValues);

        Entry entry = new Entry(dn, entryMapping, sourceValues, attributeValues);
        log.debug("\n"+EntryUtil.toString(entry));

        //Row rdn = entry.getRdn();

        //log.debug("Storing "+rdn+" in entry data cache for "+entry.getParentDn());
        //engine.getEntryCache().put(entry);

        results.add(entry);

        return results;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
