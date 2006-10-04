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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LoadEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;

    public LoadEngine(Engine engine) {
        this.engine = engine;
    }

    public void load(
            Partition partition,
            final EntryMapping entryMapping,
            final PenroseSearchResults entries,
            final PenroseSearchResults loadedEntries
    ) throws Exception {

/*
        Collection sources = entryMapping.getSourceMappings();
        Collection sourceNames = new ArrayList();
        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping sm = (SourceMapping)i.next();
            sourceNames.add(sm.getName());
        }
        log.debug("Sources: "+sourceNames);

        Collection effectiveSources = partition.getEffectiveSourceMappings(entryMapping);
        Collection effectiveSourceNames = new ArrayList();
        for (Iterator i=effectiveSources.iterator(); i.hasNext(); ) {
            SourceMapping sm = (SourceMapping)i.next();
            effectiveSourceNames.add(sm.getName());
        }
        log.debug("Effective Sources: "+effectiveSourceNames);

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

        try {
            createBatches(partition, entryMapping, entries, batches);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            batches.setReturnCode(LDAPException.OPERATIONS_ERROR);
        }

        log.debug("Loading batches.");

        try {
            loadBackground(partition, entryMapping, batches, loadedEntries);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            loadedEntries.setReturnCode(LDAPException.OPERATIONS_ERROR);
        }
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
            Partition partition,
            EntryMapping entryMapping,
            PenroseSearchResults entries,
            PenroseSearchResults batches
            ) throws Exception {

        try {
            Interpreter interpreter = engine.getInterpreterManager().newInstance();

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
                Row filter = engine.createFilter(partition, interpreter, primarySourceMapping, entryMapping, rdn);
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
            Partition partition,
            EntryMapping entryMapping,
            PenroseSearchResults batches,
            PenroseSearchResults loadedBatches
            ) throws Exception {

        //MRSWLock lock = getLock(entryMapping;
        //lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        log.info("Loading data for "+entryMapping.getDn()+".");

        try {
            while (batches.hasNext()) {
                Collection entries = (Collection)batches.next();

                AttributeValues sourceValues = new AttributeValues();
                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    EntryData data = (EntryData)i.next();
                    String dn = data.getDn();
                    AttributeValues sv = data.getMergedValues();

                    if (sv == null) continue;

                    sourceValues.add(sv);
                }

                AttributeValues sv = loadEntries(partition, sourceValues, entryMapping, entries);

                if (sv != null) {
                    for (Iterator i=entries.iterator(); i.hasNext(); ) {
                        EntryData map = (EntryData)i.next();

                        map.setLoadedSourceValues(sv);

                        loadedBatches.add(map);
                    }
                }
            }

        } finally {
            //lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            loadedBatches.setReturnCode(batches.getReturnCode());
            loadedBatches.close();
        }
    }

    public AttributeValues loadEntries(
            Partition partition,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            Collection list)
            throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("LOAD", 80));
            log.debug(Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));

            log.debug(Formatter.displayLine("Primary Keys:", 80));
            for (Iterator i=list.iterator(); i.hasNext(); ) {
                EntryData data = (EntryData)i.next();
                Row pk = data.getFilter();
                log.debug(Formatter.displayLine(" - "+pk, 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);
        log.debug("Primary source: "+(primarySourceMapping == null ? null : primarySourceMapping.getName()));

        if (primarySourceMapping == null) {
            Collection sourceNames = new TreeSet();
            for (Iterator i=parentSourceValues.getNames().iterator(); i.hasNext(); ) {
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

                AttributeValues sv = new AttributeValues(parentSourceValues);
                sv.retain(sourceName);

                newSourceValues.add(sv);
            }
*/
            return newSourceValues;
        }

        boolean pkDefined = false;
        Collection fieldMappings = primarySourceMapping.getPrimaryKeyFieldMappings();
        for (Iterator i=fieldMappings.iterator(); !pkDefined && i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();
            if (!fieldMapping.isPK()) continue;
            if (!fieldMapping.getType().equalsIgnoreCase(FieldMapping.VARIABLE)) continue;

            String attributeName = fieldMapping.getVariable();
            if (attributeName.startsWith("rdn.")) attributeName = attributeName.substring(4);

            Collection attributeMappings = entryMapping.getAttributeMappings(attributeName);
            for (Iterator j=attributeMappings.iterator(); !pkDefined && j.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)j.next();
                if (!attributeMapping.isPK()) continue;

                log.debug("PK is defined");
                pkDefined = true;
                break;
            }
        }

        Collection pks = new TreeSet();
        if (pkDefined) {
            for (Iterator i=list.iterator(); i.hasNext(); ) {
                EntryData data = (EntryData)i.next();
                Row pk = EntryUtil.getRdn(data.getDn());
                pks.add(pk);
            }
        }

        Collection filters = new ArrayList();
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            EntryData data = (EntryData)i.next();
            Row filter = data.getFilter();
            filters.add(filter);
        }
        Filter filter  = FilterTool.createFilter(filters, true);

        LoadGraphVisitor loadVisitor = new LoadGraphVisitor(
                engine,
                partition,
                entryMapping,
                parentSourceValues,
                pks,
                filter
        );

        loadVisitor.run();

        int rc = loadVisitor.getReturnCode();
        AttributeValues allSourceValues = loadVisitor.getLoadedSourceValues();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("LOAD RESULT ("+rc+")", 80));

            for (Iterator i=allSourceValues.getNames().iterator(); i.hasNext(); ) {
                String sourceName = (String)i.next();
                log.debug(Formatter.displayLine("Source "+sourceName+":", 80));

                Collection rows = allSourceValues.get(sourceName);
                for (Iterator j=rows.iterator(); j.hasNext(); ) {
                    AttributeValues av = (AttributeValues)j.next();

                    for (Iterator k=av.getNames().iterator(); k.hasNext(); ) {
                        String name = (String)k.next();
                        Collection values = av.get(name);
                        log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                    }
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        if (rc != LDAPException.SUCCESS) return null;
        
        return allSourceValues;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
