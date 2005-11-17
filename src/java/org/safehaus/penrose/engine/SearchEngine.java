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
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.SearchResults;
import org.apache.log4j.Logger;

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
            Collection path,
            final AttributeValues parentSourceValues,
            final EntryDefinition entryDefinition,
            final Filter filter,
            SearchResults entries)
            throws Exception {

        boolean unique = engine.isUnique(entryDefinition);
        log.debug("Entry "+entryDefinition.getDn()+" "+(unique ? "is" : "is not")+" unique.");

        Config config = engine.getConfig(entryDefinition.getDn());
        EntryDefinition parentDefinition = config.getParent(entryDefinition);

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();

        Collection dns = new TreeSet();

        if (path != null && path.size() > 0) {
            Map map = (Map)path.iterator().next();
            String dn = (String)map.get("dn");
            Entry parent = (Entry)map.get("entry");
            log.debug("Checking "+filter+" in entry filter cache for "+dn);

            if (parent != null) {
                String parentDn = parent.getDn();

                Collection list = engine.getCache(parentDn, entryDefinition).get(filter);
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

        final SearchResults values = new SearchResults();

        String s = engine.getEngineConfig().getParameter(EngineConfig.ALLOW_CONCURRENCY);
        boolean allowConcurrency = s == null ? true : new Boolean(s).booleanValue();

        if (allowConcurrency) {
            engine.execute(new Runnable() {
                public void run() {
                    try {
                        searchSources(parentSourceValues, entryDefinition, filter, values);

                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                    }
                }
            });
        } else {
            searchSources(parentSourceValues, entryDefinition, filter, values);
        }

        Map childDns = new HashMap();

        log.debug("Search results:");
        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sv = (AttributeValues)i.next();
            log.debug("==> "+sv);

            Collection list = engine.computeDns(interpreter, entryDefinition, sv);
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
            if (parentDefinition != null) {
                log.debug("Storing "+filter+" in entry filter cache:");
                for (Iterator i=childDns.keySet().iterator(); i.hasNext(); ) {
                    String parentDn = (String)i.next();
                    Collection c = (Collection)childDns.get(parentDn);

                    log.debug(" - "+parentDn+":");
                    for (Iterator j=c.iterator(); j.hasNext(); ) {
                        String dn = (String)j.next();
                        log.debug("   - DN: "+dn);
                    }

                    engine.getCache(parentDn, entryDefinition).put(filter, c);

                }
            }
        //}
        //filter = engineContext.getFilterTool().createFilter(rdns);
        //log.debug("Storing "+filter+" in entry filter cache for "+parentDn+" => "+rdns);
        //engineContext.getEntryFilterCache(parentDn, entryDefinition).put(filter, rdns);
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

            Entry entry = (Entry)engineContext.getCache(parentDn, entryDefinition).get(normalizedRdn);
            if (entry == null) {
                entry = new Entry(dn, entryDefinition, sv, new AttributeValues());
            }

            AttributeValues oldSv = entry.getSourceValues(); //(AttributeValues)engineContext.getEntrySourceCache(parentDn, entryDefinition).get(normalizedRdn);

            if (oldSv == null) {
                //log.debug("   Storing "+rdn+" in entry source cache.");
                engineContext.getEntrySourceCache(parentDn, entryDefinition).put(normalizedRdn, sv);
            } else {
                //log.debug("   Adding "+rdn+" in entry source cache.");
                oldSv.add(sv);
            }
            //log.debug("   source values1: "+oldSv);

            if (unique && !dns.contains(dn)) {
                //Entry entry = new Entry(dn, entryDefinition, sv, new AttributeValues());
                entries.add(entry);
                dns.add(dn);
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
            AttributeValues parentSourceValues,
            EntryDefinition entryDefinition,
            Filter filter,
            SearchResults results) throws Exception {

        SearchPlanner planner = new SearchPlanner(
                engine,
                entryDefinition,
                filter,
                parentSourceValues);

        planner.run();

        Source source = engine.getPrimarySource(entryDefinition);

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SIMPLE SEARCH", 80));
        log.debug(Formatter.displaySeparator(80));

        Map filters = planner.getFilters();
        Filter newFilter = (Filter)filters.get(source);

        String s = source.getParameter(Source.FILTER);
        if (s != null) {
            Filter sourceFilter = FilterTool.parseFilter(s);
            newFilter = FilterTool.appendAndFilter(newFilter, sourceFilter);
        }

        Config config = engine.getConfig(entryDefinition.getDn());
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        SearchResults sr = engine.getConnector().search(sourceDefinition, newFilter);

        Interpreter interpreter = engine.getInterpreterFactory().newInstance();

        //log.debug("Search Results:");
        for (Iterator i=sr.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();

            AttributeValues sv = new AttributeValues();
            sv.add(parentSourceValues);
            sv.add(source.getName(), av);

            Collection list = engine.computeDns(interpreter, entryDefinition, sv);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                String dn = (String)j.next();
                //log.debug(" - "+dn);

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
            EntryDefinition entryDefinition,
            Filter filter,
            SearchResults results)
            throws Exception {

        SearchPlanner planner = new SearchPlanner(
                engine,
                entryDefinition,
                filter,
                sourceValues);

        planner.run();

        Collection connectingSources = planner.getConnectingSources();

        Source primarySource = engine.getPrimarySource(entryDefinition);

        if (primarySource == null || entryDefinition.getSource(primarySource.getName()) == null) {
            Collection localResults = new ArrayList();
            localResults.add(sourceValues);

            Collection parentResults = searchParent(
                    entryDefinition,
                    planner,
                    localResults,
                    connectingSources
            );

            results.addAll(parentResults);
            results.close();
            return;
        }

        Collection localResults = searchLocal(
                entryDefinition,
                sourceValues,
                planner,
                connectingSources
        );

        if (localResults.isEmpty()) {
            results.close();
            return;
        }

        Collection parentResults = searchParent(
                entryDefinition,
                planner,
                localResults,
                connectingSources
        );

        results.addAll(parentResults);
        results.close();
        return;
    }

    public Collection searchLocal(
            EntryDefinition entryDefinition,
            AttributeValues sourceValues,
            SearchPlanner planner,
            Collection connectingSources) throws Exception {

        Collection results = new ArrayList();

        Source primarySource = engine.getPrimarySource(entryDefinition);

        Map filters = planner.getFilters();

        Map map = null;
        for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            Filter toFilter = (Filter)filters.get(toSource);
            Filter tf = engine.generateFilter(toSource, relationships, sourceValues);
            toFilter = FilterTool.appendAndFilter(toFilter, tf);
            filters.put(toSource, toFilter);

            log.debug("Filter for "+toSource.getName()+": "+toFilter);

            if (toFilter == null && map == null) continue;

            map = m;
        }

        if (map == null) {
            // if there's no parent source that can be used as filters
            // start from any local source that has a filter

            for (Iterator i=entryDefinition.getSources().iterator(); i.hasNext(); ) {
                Source source = (Source)i.next();
                Filter f = (Filter)filters.get(source);
                if (f == null) continue;

                map = new HashMap();
                map.put("toSource", source);
                map.put("relationships", new ArrayList());

                break;
            }
        }

        // start from the primary source
        if (map == null) {
            map = new HashMap();
            map.put("toSource", primarySource);
            map.put("relationships", new ArrayList());
        }

        Source startingSource = (Source)map.get("toSource");
        Collection relationships = (Collection)map.get("relationships");

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH LOCAL", 80));
        log.debug(Formatter.displayLine("Parent source values:", 80));

        for (Iterator j=sourceValues.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Collection v = sourceValues.get(name);
            log.debug(Formatter.displayLine(" - "+name+": "+v, 80));
        }

        log.debug(Formatter.displayLine("Starting source: "+startingSource.getName(), 80));
        log.debug(Formatter.displayLine("Relationships:", 80));

        for (Iterator j=relationships.iterator(); j.hasNext(); ) {
            Relationship relationship = (Relationship)j.next();
            log.debug(Formatter.displayLine(" - "+relationship, 80));
        }

        log.debug(Formatter.displaySeparator(80));

        SearchLocalRunner runner = new SearchLocalRunner(
                engine,
                entryDefinition,
                sourceValues,
                planner,
                startingSource,
                relationships);

        runner.run();

        Collection list = runner.getResults();

        SearchCleaner cleaner = new SearchCleaner(
                engine,
                entryDefinition,
                planner,
                primarySource);

        for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();
            Source source = (Source)m.get("toSource");
            cleaner.run(source);
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
            EntryDefinition entryDefinition,
            SearchPlanner planner,
            Collection localResults,
            Collection startingSources) throws Exception {

        Collection results = new ArrayList();

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH PARENT", 80));

        //log.debug(Formatter.displayLine("Local source values:", 80));

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=localResults.iterator(); i.hasNext(); ) {
            AttributeValues sv = (AttributeValues)i.next();
            sourceValues.add(sv);

            for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = sv.get(name);
                //log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));

        Map filters = planner.getFilters();
        
        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            Filter fromFilter = (Filter)filters.get(fromSource);
            Filter tf = engine.generateFilter(fromSource, relationships, sourceValues);
            fromFilter = FilterTool.appendAndFilter(fromFilter, tf);
            filters.put(fromSource, fromFilter);

            log.debug("Filter for "+fromSource.getName()+": "+fromFilter);
        }

        if (startingSources.isEmpty()) {
            Config config = engine.getConfig(entryDefinition.getDn());
            EntryDefinition parentDefinition = config.getParent(entryDefinition);

            while (parentDefinition != null) {
                log.debug("Checking: "+parentDefinition.getDn());

                Source source = engine.getPrimarySource(parentDefinition);
                log.debug("Primary source: "+source);

                if (source != null) {
                    Map map = new HashMap();
                    map.put("fromSource", source);
                    map.put("relationships", new ArrayList());

                    startingSources.add(map);
                    break;
                }

                parentDefinition = config.getParent(parentDefinition);
            }

            if (parentDefinition == null) {
                results.add(new AttributeValues());
            }
        }

        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            log.debug("Starting source: "+fromSource.getName());
            log.debug("Relationships:");

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
                log.debug(" - "+relationship);
            }

            Filter filter = (Filter)filters.get(fromSource);
            log.debug("Filter: "+filter);

            SearchParentRunner runner = new SearchParentRunner(
                    engine,
                    entryDefinition,
                    localResults,
                    sourceValues,
                    planner,
                    fromSource,
                    relationships);

            runner.run();

            results.addAll(runner.getResults());
        }

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH PARENT RESULTS", 80));

        int counter = 1;
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

        return results;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
