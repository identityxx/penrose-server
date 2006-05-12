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
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.partition.Partition;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LoadEngine {

    Logger log = Logger.getLogger(getClass());

    private Engine engine;

    public LoadEngine(Engine engine) {
        this.engine = engine;
    }

    public void load(
            final EntryMapping entryMapping,
            final PenroseSearchResults entries,
            final PenroseSearchResults loadedEntries
            ) throws Exception {

        Partition partition = engine.getPartitionManager().getPartition(entryMapping);

        Collection sources = entryMapping.getSourceMappings();
        log.debug("Sources: "+sources);

        Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);
        log.debug("Effective Sources: "+effectiveSources);
/*
        if (sources.size() == 0 && effectiveSources.size() == 0 || sources.size() == 1 && effectiveSources.size() == 1) {

            log.debug("All sources have been loaded.");

            engine.execute(new Runnable() {
                public void run() {
                    try {
                        checkCache(entryMapping, entries, loadedEntries);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        loadedEntries.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    }
                }
            });

            return;
        }
*/
        log.debug("Creating batches of entries.");

        final PenroseSearchResults batches = new PenroseSearchResults();

        engine.threadManager.execute(new Runnable() {
            public void run() {
                try {
                    createBatches(entryMapping, entries, batches);

                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                    batches.setReturnCode(org.ietf.ldap.LDAPException.OPERATIONS_ERROR);
                }
            }
        });

        log.debug("Loading batches.");

        engine.threadManager.execute(new Runnable() {
            public void run() {
                try {
                    loadBackground(entryMapping, batches, loadedEntries);

                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                    loadedEntries.setReturnCode(org.ietf.ldap.LDAPException.OPERATIONS_ERROR);
                }
            }
        });
    }

    public void checkCache(EntryMapping entryMapping, PenroseSearchResults entries, PenroseSearchResults loadedEntries) throws Exception {
        for (Iterator i=entries.iterator(); i.hasNext(); ) {
            EntryData map = (EntryData)i.next();
/*
            String dn = (String)map.get("dn");

            String parentDn = Entry.getParentDn(dn);
            Row rdn = Entry.getRdn(dn);

            log.debug("Checking "+rdn+" in entry data cache for "+parentDn);
            Entry entry = (Entry)engine.getEntryCache().get(entryMapping, parentDn, rdn);

            if (entry != null) {
                log.debug("Entry "+rdn+" has been loaded");
                results.add(entry);
                continue;
            }
*/
            loadedEntries.add(map);
        }
        loadedEntries.setReturnCode(entries.getReturnCode());
        loadedEntries.close();

    }

    public void createBatches(
            EntryMapping entryMapping,
            PenroseSearchResults entries,
            PenroseSearchResults batches
            ) throws Exception {

        try {
            Interpreter interpreter = engine.getInterpreterFactory().newInstance();

            SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

            Collection batch = new ArrayList();

            String s = entryMapping.getParameter(EntryMapping.BATCH_SIZE);
            int batchSize = s == null ? EntryMapping.DEFAULT_BATCH_SIZE : Integer.parseInt(s);

            for (Iterator i=entries.iterator(); i.hasNext(); ) {
                EntryData map = (EntryData)i.next();
                String dn = map.getDn();
                //AttributeValues sv = (AttributeValues)map.get("sourceValues");

                Row rdn = EntryUtil.getRdn(dn);
/*
                if (partition.getParent(entryMapping) != null) {
                    String parentDn = Entry.getParentDn(dn);

                    log.debug("Checking "+rdn+" in entry data cache for "+parentDn);
                    Entry entry = (Entry)engine.getEntryCache().get(entryMapping, parentDn, rdn);

                    if (entry != null) {
                        log.debug(" - "+rdn+" has been loaded");
                        results.add(entry);
                        continue;
                    }
                }
*/
                Row filter = engine.createFilter(interpreter, primarySourceMapping, entryMapping, rdn);
                if (filter == null) continue;

                //if (filter.isEmpty()) filter.add(rdn);

                //log.info("Scheduling "+rdn+" for loading");
                map.setFilter(filter);
                batch.add(map);

                if (batch.size() < batchSize) continue;

                batches.add(batch);
                batch = new ArrayList();
            }

            if (!batch.isEmpty()) batches.add(batch);

        } finally {
            batches.setReturnCode(entries.getReturnCode());
            batches.close();
        }
    }

    public void loadBackground(
            EntryMapping entryMapping,
            PenroseSearchResults batches,
            PenroseSearchResults loadedBatches
            ) throws Exception {

        //MRSWLock lock = getLock(entryMapping;
        //lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {
            while (batches.hasNext()) {
                Collection entries = (Collection)batches.next();

                log.info(Formatter.displaySeparator(80));
                log.info(Formatter.displayLine("LOAD", 80));
                log.info(Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));

                AttributeValues sourceValues = new AttributeValues();
                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    EntryData map = (EntryData)i.next();
                    String dn = map.getDn();
                    AttributeValues sv = map.getMergedValues();
                    Collection rows = map.getRows();
                    Row filter = map.getFilter();

                    log.debug(Formatter.displayLine(" - "+dn, 80));
                    log.debug(Formatter.displayLine("   filter: "+filter, 80));

                    if (sv == null) continue;

                    sourceValues.add(sv);

                    for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                        String name = (String)j.next();
                        Collection values = sv.get(name);
                        log.debug(Formatter.displayLine("   - "+name+": "+values, 80));
                    }

                    log.debug(Formatter.displayLine("   rows:", 80));
                    if (rows != null) {
                        int counter = 0;
                        for (Iterator j=rows.iterator(); j.hasNext() && counter <= 20; counter++) {
                            AttributeValues row = (AttributeValues)j.next();
                            log.debug(Formatter.displayLine("   - "+row, 80));
                        }
                    }
                }

                log.info(Formatter.displaySeparator(80));

                AttributeValues loadedSourceValues = loadEntries(sourceValues, entryMapping, entries);

                if (log.isDebugEnabled()) {
                    log.debug(Formatter.displaySeparator(80));
                    log.debug(Formatter.displayLine("LOAD RESULT", 80));

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

                    log.debug(Formatter.displaySeparator(80));
                }

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    EntryData map = (EntryData)i.next();

                    map.setLoadedSourceValues(loadedSourceValues);

                    loadedBatches.add(map);
                }
            }

        } finally {
            //lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            loadedBatches.setReturnCode(batches.getReturnCode());
            loadedBatches.close();
        }
    }

    public AttributeValues loadEntries(
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            Collection maps)
            throws Exception {

        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);
        log.debug("Primary source: "+(primarySourceMapping == null ? null : primarySourceMapping.getName()));

        if (primarySourceMapping == null) {
            Collection sourceNames = new TreeSet();
            for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                int index = name.indexOf(".");
                String sourceName = name.substring(0, index);
                sourceNames.add(sourceName);
            }

            AttributeValues newSourceValues = new AttributeValues();
/*
            for (Iterator i=sourceNames.iterator(); i.hasNext(); ) {
                String sourceName = (String)i.next();
                if ("parent".equals(sourceName)) continue;

                AttributeValues sv = new AttributeValues(sourceValues);
                sv.retain(sourceName);

                newSourceValues.add(sv);
            }
*/
            return newSourceValues;
        }

        Collection pks = new TreeSet();
        for (Iterator i=maps.iterator(); i.hasNext(); ) {
            EntryData m = (EntryData)i.next();
            //String dn = (String)m.get("dn");
            //AttributeValues sv = (AttributeValues)m.get("sourceValues");
            Row pk = m.getFilter();
            pks.add(pk);
        }

        Filter filter  = FilterTool.createFilter(pks, true);

        Map map = new HashMap();
        map.put("attributeValues", sourceValues);
        map.put("filter", filter);

        Collection filters = new ArrayList();
        filters.add(map);

        LoadGraphVisitor loadVisitor = new LoadGraphVisitor(engine, entryMapping, sourceValues, filter);
        loadVisitor.run();

        return loadVisitor.getLoadedSourceValues();
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
