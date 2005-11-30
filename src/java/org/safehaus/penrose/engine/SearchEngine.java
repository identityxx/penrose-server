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
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connector.ConnectionConfig;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = Logger.getLogger(getClass());

    private Engine engine;

    public SearchEngine(Engine engine) {
        this.engine = engine;
    }

    public void search(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final PenroseSearchResults entries
            ) throws Exception {

        boolean staticEntry = engine.isStatic(entryMapping);
        if (staticEntry) {
            searchStatic(path, parentSourceValues, entryMapping, filter, entries);
            return;
        }

        boolean unique = engine.isUnique(entryMapping);
        log.debug("Entry "+entryMapping.getDn()+" "+(unique ? "is" : "is not")+" unique.");

        Partition partition = engine.getPartitionManager().getPartition(entryMapping);

        Collection sources = entryMapping.getSourceMappings();
        log.debug("Sources: "+sources);

        Collection effectiveSources = partition.getEffectiveSources(entryMapping);
        log.debug("Effective Sources: "+effectiveSources);

        if (unique && sources.size() == 1 && effectiveSources.size() == 1) {
            simpleSearch(parentSourceValues, entryMapping, filter, entries);
            return;
        }

        searchDynamic(path, parentSourceValues, entryMapping, filter, entries);
    }

    public void searchStatic(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final PenroseSearchResults entries
            ) throws Exception {

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();

        Collection list = engine.computeDns(interpreter, entryMapping, parentSourceValues);
        for (Iterator j=list.iterator(); j.hasNext(); ) {
            String dn = (String)j.next();
            log.debug(" - "+dn);

            Map map = new HashMap();
            map.put("dn", entryMapping.getDn());
            map.put("sourceValues", parentSourceValues);
            entries.add(map);
        }
        entries.close();
    }

    public void searchDynamic(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final PenroseSearchResults entries
            ) throws Exception {

        String s = engine.getEngineConfig().getParameter(EngineConfig.ALLOW_CONCURRENCY);
        boolean allowConcurrency = s == null ? true : new Boolean(s).booleanValue();

        if (allowConcurrency) {
            engine.execute(new Runnable() {
                public void run() {
                    try {
                        searchDynamicBackground(path, parentSourceValues, entryMapping, filter, entries);

                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        entries.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    }
                }
            });
        } else {
            searchDynamicBackground(path, parentSourceValues, entryMapping, filter, entries);
        }
    }

    public void searchDynamicBackground(
            Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            PenroseSearchResults entries)
            throws Exception {

        //boolean unique = engine.isUnique(entryMapping  //log.debug("Entry "+entryMapping" "+(unique ? "is" : "is not")+" unique.");

        Partition partition = engine.getPartitionManager().getPartition(entryMapping);
        EntryMapping parentMapping = partition.getParent(entryMapping);

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();

        Collection dns = new TreeSet();

        if (path != null && path.size() > 0) {
            Map map = (Map)path.iterator().next();
            String dn = (String)map.get("dn");
            Entry parent = (Entry)map.get("entry");
            log.debug("Checking "+filter+" in entry filter cache for "+dn);

            if (parent != null) {
                String parentDn = parent.getDn();

                Collection list = engine.getCache(parentDn, entryMapping).get(filter);
                if (list != null) dns.addAll(list);
            }
        } else {
            log.debug("Entry has no parent");
        }

        //log.debug("DNs: "+dns);

        Map results = new TreeMap();

        if (!dns.isEmpty()) {
            log.debug("Filter cache found:");

            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                String dn = (String)i.next();
                AttributeValues sv = new AttributeValues();

                log.debug(" - "+dn);
                results.put(dn, sv);

                Map map = new HashMap();
                map.put("dn", dn);
                map.put("sourceValues", sv);
                entries.add(map);
            }

            entries.close();
            return;
        }
        
        log.debug("Filter cache does not contain filter "+filter);

        final PenroseSearchResults values = new PenroseSearchResults();

        String s = engine.getEngineConfig().getParameter(EngineConfig.ALLOW_CONCURRENCY);
        boolean allowConcurrency = s == null ? true : new Boolean(s).booleanValue();

        if (allowConcurrency) {
            engine.execute(new Runnable() {
                public void run() {
                    try {
                        searchSources(parentSourceValues, entryMapping, filter, values);

                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                    }
                }
            });
        } else {
            searchSources(parentSourceValues, entryMapping, filter, values);
        }

        Map childDns = new HashMap();

        log.debug("Search results:");
        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sv = (AttributeValues)i.next();
            //log.debug("==> "+sv);

            Collection list = engine.computeDns(interpreter, entryMapping, sv);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                String dn = (String)j.next();
                log.debug("    - "+dn);

                dns.add(dn);

                Row rdn = Entry.getRdn(dn);
                Row normalizedRdn = getEngine().getSchema().normalize(rdn);
                String parentDn = Entry.getParentDn(dn);

                AttributeValues av = (AttributeValues)results.get(dn);
                if (av == null) {
                    av = new AttributeValues();
                    results.put(dn, av);
                }
                av.add(sv);
/*
                if (unique) {
                    Map map = new HashMap();
                    map.put("dn", dn);
                    map.put("sourceValues", sv);
                    entries.add(map);
                }
*/
                Collection c = (Collection)childDns.get(parentDn);
                if (c == null) {
                    c = new TreeSet();
                    childDns.put(parentDn, c);
                }
                c.add(dn);
            }
        }

        //if (!unique) {
            if (parentMapping != null) {
                log.debug("Storing "+filter+" in entry filter cache:");
                for (Iterator i=childDns.keySet().iterator(); i.hasNext(); ) {
                    String parentDn = (String)i.next();
                    Collection c = (Collection)childDns.get(parentDn);

                    log.debug(" - "+parentDn+":");
                    for (Iterator j=c.iterator(); j.hasNext(); ) {
                        String dn = (String)j.next();
                        log.debug("   - DN: "+dn);
                    }

                    engine.getCache(parentDn, entryMapping).put(filter, c);

                }
            }
        //}
        //filter = engineContext.getFilterTool().createFilter(rdns);
        //log.debug("Storing "+filter+" in entry filter cache for "+parentDn+" => "+rdns);
        //engineContext.getEntryFilterCache(parentDn, entryMappingter, rdns);
/*
        log.debug("Getting source values:");

        for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
            String dn = (String)i.next();
            log.debug(" - "+dn);

            AttributeValues sv = (AttributeValues)results.get(dn);
            if (sv != null) continue;

            Row rdn = Entry.getRdn(dn);
            Row normalizedRdn = getEngineContext().getSchema().normalize(rdn);
            String parentDn = Entry.getParentDn(dn);

            Entry entry = (Entry)engineContext.getCache(parentDn, entryMapping  entry = new Entry(dn, entryMappingues oldSv = entry.getSourceValues(); //(AttributeValues)engineContext.getEntrySourceCache(parentDn, entryMapping   //log.debug("   Storing "+rdn+" in entry source cache.");
                engineContext.getEntrySourceCache(parentDn, entryMappingdebug("   Adding "+rdn+" in entry source cache.");
                oldSv.add(sv);
            }
            //log.debug("   source values1: "+oldSv);

            if (unique && !dns.contains(dn)) {
                //Entry entry = new Entry(dn, entryMapping             dns.add(dn);
            }
            //log.debug("   source values2: "+av);

        }
*/
        //if (!unique) {
            for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
                String dn = (String)i.next();
                AttributeValues sv = (AttributeValues)results.get(dn);

                Map map = new HashMap();
                map.put("dn", dn);
                map.put("sourceValues", sv);
                entries.add(map);
            }
        //}

        entries.close();
    }

    public void simpleSearch(
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final PenroseSearchResults results) throws Exception {

        String s = engine.getEngineConfig().getParameter(EngineConfig.ALLOW_CONCURRENCY);
        boolean allowConcurrency = s == null ? true : new Boolean(s).booleanValue();

        if (allowConcurrency) {
            engine.execute(new Runnable() {
                public void run() {
                    try {
                        simpleSearchBackground(parentSourceValues, entryMapping, filter, results);

                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        results.setReturnCode(LDAPException.OPERATIONS_ERROR);
                    }
                }
            });
        } else {
            simpleSearchBackground(parentSourceValues, entryMapping, filter, results);
        }
    }

    public void simpleSearchBackground(
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            Filter filter,
            PenroseSearchResults results) throws Exception {

        SearchPlanner planner = new SearchPlanner(
                engine,
                entryMapping,
                filter,
                parentSourceValues);

        planner.run();

        SourceMapping sourceMapping = engine.getPrimarySource(entryMapping);

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SIMPLE SEARCH", 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Map filters = planner.getFilters();
        Filter newFilter = (Filter)filters.get(sourceMapping);

        String s = sourceMapping.getParameter(SourceMapping.FILTER);
        if (s != null) {
            Filter sourceFilter = FilterTool.parseFilter(s);
            newFilter = FilterTool.appendAndFilter(newFilter, sourceFilter);
        }

        Partition partition = engine.getPartitionManager().getPartition(entryMapping);
        ConnectionConfig connectionConfig = partition.getConnectionConfig(sourceMapping.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(sourceMapping.getSourceName());

        PenroseSearchResults sr = engine.getConnector().search(sourceDefinition, newFilter);

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();

        log.debug("Search Results:");
        for (Iterator i=sr.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            AttributeValues sv = new AttributeValues();
            sv.add(parentSourceValues);
            sv.add(sourceMapping.getName(), av);

            Collection list = engine.computeDns(interpreter, entryMapping, sv);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                String dn = (String)j.next();
                log.debug(" - "+dn);

                Map map = new HashMap();
                map.put("dn", dn);
                map.put("sourceValues", sv);
                results.add(map);
            }
        }

        results.close();
    }

    public void searchSources(
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            Filter filter,
            PenroseSearchResults results)
            throws Exception {

        SearchPlanner planner = new SearchPlanner(
                engine,
                entryMapping,
                filter,
                sourceValues);

        planner.run();

        Collection connectingSources = planner.getConnectingSources();

        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        if (primarySourceMapping == null || entryMapping.getSourceMapping(primarySourceMapping.getName()) == null) {
            Collection localResults = new ArrayList();
            localResults.add(sourceValues);

            Collection parentResults = searchParent(
                    entryMapping,
                    planner,
                    localResults,
                    connectingSources
            );

            results.addAll(parentResults);
            results.close();
            return;
        }

        Collection localResults = searchLocal(
                entryMapping,
                sourceValues,
                planner,
                connectingSources
        );

        if (localResults.isEmpty()) {
            results.close();
            return;
        }

        Collection parentResults = searchParent(
                entryMapping,
                planner,
                localResults,
                connectingSources
        );

        results.addAll(parentResults);
        results.close();
        return;
    }

    public Collection searchLocal(
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            SearchPlanner planner,
            Collection connectingSources) throws Exception {

        Collection results = new ArrayList();

        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        Map filters = planner.getFilters();

        Map map = null;
        for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            SourceMapping fromSourceMapping = (SourceMapping)m.get("fromSource");
            SourceMapping toSourceMapping = (SourceMapping)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            Filter toFilter = (Filter)filters.get(toSourceMapping);
            Filter tf = engine.generateFilter(toSourceMapping, relationships, sourceValues);
            toFilter = FilterTool.appendAndFilter(toFilter, tf);
            filters.put(toSourceMapping, toFilter);

            log.debug("Filter for "+toSourceMapping.getName()+": "+toFilter);

            if (toFilter == null && map == null) continue;

            map = m;
        }

        if (map == null) {
            // if there's no parent source that can be used as filters
            // start from any local source that has a filter

            for (Iterator i=entryMapping.getSourceMappings().iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                Filter f = (Filter)filters.get(sourceMapping);
                if (f == null) continue;

                map = new HashMap();
                map.put("toSource", sourceMapping);
                map.put("relationships", new ArrayList());

                break;
            }
        }

        // start from the primary source
        if (map == null) {
            map = new HashMap();
            map.put("toSource", primarySourceMapping);
            map.put("relationships", new ArrayList());
        }

        SourceMapping startingSourceMapping = (SourceMapping)map.get("toSource");
        Collection relationships = (Collection)map.get("relationships");

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH LOCAL", 80));
            log.debug(Formatter.displayLine("Parent source values:", 80));

            for (Iterator j=sourceValues.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection v = sourceValues.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+v, 80));
            }

            log.debug(Formatter.displayLine("Starting source: "+startingSourceMapping.getName(), 80));
            log.debug(Formatter.displayLine("Relationships:", 80));

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
                log.debug(Formatter.displayLine(" - "+relationship, 80));
            }

            log.debug(Formatter.displaySeparator(80));
        }

        SearchLocalRunner runner = new SearchLocalRunner(
                engine,
                entryMapping,
                sourceValues,
                planner,
                startingSourceMapping,
                relationships);

        runner.run();

        Collection list = runner.getResults();

        SearchCleaner cleaner = new SearchCleaner(
                engine,
                entryMapping,
                planner,
                primarySourceMapping);

        for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();
            SourceMapping sourceMapping = (SourceMapping)m.get("toSource");
            cleaner.run(sourceMapping);
        }

        cleaner.clean(list);

        results.addAll(list);
/*
        log.debug("Search local results:");

        int counter = 1;
        for (Iterator i=results.iterator(); i.hasNext(); counter++) {
            AttributeValues av = (AttributeValues)i.next();
            log.debug("Result #"+counter);
            for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection v = av.get(name);
                log.debug(" - "+name+": "+v);
            }
        }
*/
        return results;
    }

    public Collection searchParent(
            EntryMapping entryMapping,
            SearchPlanner planner,
            Collection localResults,
            Collection startingSources) throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH PARENT", 80));

        log.debug(Formatter.displayLine("Local source values:", 80));

        AttributeValues sourceValues = new AttributeValues();
        int counter = 1;
        for (Iterator i=localResults.iterator(); i.hasNext(); counter++) {
            AttributeValues sv = (AttributeValues)i.next();
            sourceValues.add(sv);

            log.debug(Formatter.displayLine("Record #"+counter, 80));
            for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = sv.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));

        Collection results = new ArrayList();
        results.addAll(localResults);

        Map filters = planner.getFilters();
        
        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            SourceMapping fromSourceMapping = (SourceMapping)m.get("fromSource");
            SourceMapping toSourceMapping = (SourceMapping)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            Filter fromFilter = (Filter)filters.get(fromSourceMapping);
            Filter tf = engine.generateFilter(fromSourceMapping, relationships, sourceValues);
            fromFilter = FilterTool.appendAndFilter(fromFilter, tf);
            filters.put(fromSourceMapping, fromFilter);

            log.debug("Filter for "+fromSourceMapping.getName()+": "+fromFilter);
        }

        if (startingSources.isEmpty()) {
            Partition partition = engine.getPartitionManager().getPartition(entryMapping);
            EntryMapping parentMapping = partition.getParent(entryMapping);

            while (parentMapping != null) {
                log.debug("Checking: "+parentMapping.getDn());

                SourceMapping sourceMapping = engine.getPrimarySource(parentMapping);
                log.debug("Primary source: "+sourceMapping);

                if (sourceMapping != null) {
                    Map map = new HashMap();
                    map.put("fromSource", sourceMapping);
                    map.put("relationships", new ArrayList());

                    startingSources.add(map);
                    break;
                }

                parentMapping = partition.getParent(parentMapping);
            }

            if (parentMapping == null) {
                results.add(new AttributeValues());
            }
        }

        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            SourceMapping fromSourceMapping = (SourceMapping)m.get("fromSource");
            SourceMapping toSourceMapping = (SourceMapping)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            log.debug("Starting source: "+fromSourceMapping.getName());
            log.debug("Relationships:");

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
                log.debug(" - "+relationship);
            }

            Filter filter = (Filter)filters.get(fromSourceMapping);
            log.debug("Filter: "+filter);

            SearchParentRunner runner = new SearchParentRunner(
                    engine,
                    entryMapping,
                    results,
                    sourceValues,
                    planner,
                    fromSourceMapping,
                    relationships);

            runner.run();
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH PARENT RESULTS", 80));

            counter = 1;
            for (Iterator j=results.iterator(); j.hasNext(); counter++) {
                AttributeValues av = (AttributeValues)j.next();
                log.debug(Formatter.displayLine("Result #"+counter, 80));
                for (Iterator k=av.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Collection values = av.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return results;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
