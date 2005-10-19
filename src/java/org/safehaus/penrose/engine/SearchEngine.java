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
import org.safehaus.penrose.util.Formatter;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = Logger.getLogger(getClass());

    private Engine engine;
    private EngineContext engineContext;

    public SearchEngine(Engine engine) {
        this.engine = engine;
        this.engineContext = engine.getEngineContext();
    }

    public Map search(
            Collection parents,
            EntryDefinition entryDefinition,
            Filter filter)
            throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH", 80));
        log.debug(Formatter.displayLine("Entry: "+entryDefinition.getDn(), 80));
        log.debug(Formatter.displayLine("Filter: "+filter, 80));
        log.debug(Formatter.displayLine("Parents:", 80));

        Collection parentSourceValues = new TreeSet();
        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=parents.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            log.debug(Formatter.displayLine(" - "+entry.getDn(), 80));

            AttributeValues sv = entry.getSourceValues();
            sourceValues.add(sv);
            parentSourceValues.add(sv);

            for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = sv.get(name);
                log.debug(Formatter.displayLine("   - "+name+": "+values, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));

        Collection dns = new TreeSet();
        for (Iterator i=parents.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            String parentDn = entry.getDn();

            log.debug("Checking "+filter+" in entry filter cache for "+parentDn);
            Collection list = engineContext.getEntryFilterCache(parentDn, entryDefinition).get(filter);
            if (list != null) dns.addAll(list);
        }

        Map results = new TreeMap();

        if (dns.isEmpty()) {
            log.debug("Cache not found.");

            Collection values = searchEntries(parentSourceValues, entryDefinition, filter);

            Map map2 = new HashMap();

            log.debug("Search results:");
            for (Iterator i=values.iterator(); i.hasNext(); ) {
                AttributeValues sv = (AttributeValues)i.next();

                Collection list = engine.computeDns(entryDefinition, sv);
                for (Iterator j=list.iterator(); j.hasNext(); ) {
                    String dn = (String)j.next();
                    log.debug(" - "+dn);

                    int index = dn.indexOf(",");
                    String rdn = dn.substring(0, index);
                    String parentDn = dn.substring(index+1);

                    index = rdn.indexOf("=");
                    String rdnAttr = rdn.substring(0, index);
                    String rdnValue = rdn.substring(index+1);

                    Row row = new Row();
                    row.set(rdnAttr, rdnValue);

                    log.debug("   Storing "+rdn+" in entry source cache.");
                    engineContext.getEntrySourceCache(parentDn, entryDefinition).put(row, sv);

                    AttributeValues av = (AttributeValues)results.get(dn);
                    if (av == null) {
                        av = new AttributeValues();
                        results.put(dn, av);
                    }
                    av.add(sv);

                    Collection c = (Collection)map2.get(parentDn);
                    if (c == null) {
                        c = new TreeSet();
                        map2.put(parentDn, c);
                    }
                    c.add(dn);
                }
            }

            log.debug("Storing "+filter+" in entry filter cache:");
            for (Iterator i=map2.keySet().iterator(); i.hasNext(); ) {
                String parentDn = (String)i.next();
                Collection c = (Collection)map2.get(parentDn);

                log.debug(" - "+parentDn+":");
                for (Iterator j=c.iterator(); j.hasNext(); ) {
                    String dn = (String)j.next();
                    log.debug("   - "+dn);
                }

                engineContext.getEntryFilterCache(parentDn, entryDefinition).put(filter, c);

            }
            //filter = engineContext.getFilterTool().createFilter(rdns);
            //log.debug("Storing "+filter+" in entry filter cache for "+parentDn+" => "+rdns);
            //engineContext.getEntryFilterCache(parentDn, entryDefinition).put(filter, rdns);

        } else {
            log.debug("Cache found: "+dns);

            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                String dn = (String)i.next();

                int index = dn.indexOf(",");
                String s = dn.substring(0, index);
                String parentDn = dn.substring(index+1);

                index = s.indexOf("=");
                Row rdn = new Row();
                rdn.set(s.substring(0, index), s.substring(index+1));

                log.debug("Getting "+rdn+" from entry source cache for "+parentDn);
                AttributeValues sv = (AttributeValues)engineContext.getEntrySourceCache(parentDn, entryDefinition).get(rdn);
                log.debug("Entry source cache: "+sv);

                AttributeValues av = (AttributeValues)results.get(dn);
                if (av == null) {
                    av = new AttributeValues();
                    results.put(dn, av);
                }
                av.add(sv);
            }
        }
/*
        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH RESULTS", 80));
        log.debug(Formatter.displayLine("Entry: "+entryDefinition.getDn(), 80));
        log.debug(Formatter.displayLine("Results:", 80));

        for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
            String dn = (String)i.next();
            AttributeValues sv = (AttributeValues)results.get(dn);
            log.debug(Formatter.displayLine(" - "+dn, 80));
            log.debug(Formatter.displayLine("   "+sv, 80));
        }

        log.debug(Formatter.displaySeparator(80));
*/
        return results;
    }

    public Collection searchEntries(
            Collection parentSourceValues,
            EntryDefinition entryDefinition,
            Filter filter)
            throws Exception {

        Source primarySource = engine.getPrimarySource(entryDefinition);

        if (primarySource == null || entryDefinition.getSource(primarySource.getName()) == null) {
            return parentSourceValues;
        }

        SearchPlanner planner = new SearchPlanner(
                engine,
                entryDefinition,
                filter,
                parentSourceValues);

        planner.run();

        Collection connectingSources = planner.getConnectingSources();
        Collection connectingRelationships = planner.getConnectingRelationships();

        Collection results = searchLocal(
                entryDefinition,
                parentSourceValues,
                planner,
                connectingSources
        );
/*
        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Results after searching local:", 80));

        for (Iterator i=results.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();
            log.debug(Formatter.displayLine(" - "+av, 80));
            for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection v = av.get(name);
                log.debug(Formatter.displayLine("   "+name+": "+v, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));
*/
        if (results.isEmpty()) return results;

        Collection parentResults = searchParent(
                entryDefinition,
                parentSourceValues,
                planner,
                results,
                connectingSources
        );

        Collection values = engine.getJoinEngine().leftJoin(results, parentResults, connectingRelationships);
/*
        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Results after searching parents:", 80));

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues av = (AttributeValues)i.next();
            log.debug(Formatter.displayLine(" - "+av, 80));
            for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection v = av.get(name);
                log.debug(Formatter.displayLine("   "+name+": "+v, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));
*/
        return values;
    }

    public Collection searchLocal(
            EntryDefinition entryDefinition,
            Collection parentSourceValues,
            SearchPlanner planner,
            Collection connectingSources) throws Exception {

        Collection results = new ArrayList();

        Source primarySource = engine.getPrimarySource(entryDefinition);

        Map filters = planner.getFilters();

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=parentSourceValues.iterator(); i.hasNext(); ) {
            AttributeValues sv = (AttributeValues)i.next();
            sourceValues.add(sv);
        }

        Map map = null;
        for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            Filter toFilter = (Filter)filters.get(toSource);
            Filter tf = engine.generateFilter(toSource, relationships, sourceValues);
            toFilter = engineContext.getFilterTool().appendAndFilter(toFilter, tf);
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

            // if there isn't any local source that has a filter, start from the primary source

            if (map == null) {
                map = new HashMap();
                map.put("toSource", primarySource);
                map.put("relationships", new ArrayList());
            }

            Source toSource = (Source)map.get("toSource");
            Collection relationships = (Collection)map.get("relationships");

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Searching source "+primarySource.getName(), 80));
            log.debug(Formatter.displayLine("Relationships:", 80));

            log.debug(Formatter.displaySeparator(80));

            SearchLocalRunner runner = new SearchLocalRunner(
                    engine,
                    entryDefinition,
                    parentSourceValues,
                    planner.getFilters(),
                    planner.getDepths(),
                    toSource,
                    relationships);

            runner.run();

            Collection list = runner.getResults();

            SearchCleaner cleaner = new SearchCleaner(
                    engine,
                    entryDefinition,
                    planner.getFilters(),
                    planner.getDepths(),
                    primarySource);

            for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
                Map m = (Map)i.next();

                Source source = (Source)m.get("toSource");

                cleaner.run(source);
            }

            cleaner.clean(list);

            results.addAll(list);

        } else {

            Source toSource = (Source)map.get("toSource");
            Collection relationships = (Collection)map.get("relationships");

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search local sources starting from "+toSource.getName(), 80));
            log.debug(Formatter.displayLine("Relationships:", 80));

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
                log.debug(Formatter.displayLine(" - "+relationship, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            SearchPrimarySourceRunner runner = new SearchPrimarySourceRunner(
                    engine,
                    entryDefinition,
                    parentSourceValues,
                    planner.getFilters(),
                    planner.getDepths(),
                    toSource,
                    relationships);

            runner.run();

            results.addAll(runner.getResults());
        }
        
        return results;
    }

    public Collection searchParent(
            EntryDefinition entryDefinition,
            Collection parentSourceValues,
            SearchPlanner planner,
            Collection results,
            Collection startingSources) throws Exception {

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=results.iterator(); i.hasNext(); ) {
            AttributeValues sv = (AttributeValues)i.next();
            sourceValues.add(sv);
        }

        Map filters = planner.getFilters();
        
        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            Filter fromFilter = (Filter)filters.get(fromSource);
            Filter tf = engine.generateFilter(fromSource, relationships, sourceValues);
            fromFilter = engineContext.getFilterTool().appendAndFilter(fromFilter, tf);
            filters.put(fromSource, fromFilter);

            log.debug("Filter for "+fromSource.getName()+": "+fromFilter);
        }

        Collection parentResults = new ArrayList();

        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search parent sources starting from "+fromSource.getName(), 80));
            log.debug(Formatter.displayLine("Relationships:", 80));

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
                log.debug(Formatter.displayLine(" - "+relationship, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            SearchParentRunner runner = new SearchParentRunner(
                    engine,
                    entryDefinition,
                    parentSourceValues,
                    planner.getFilters(),
                    planner.getDepths(),
                    fromSource,
                    relationships);

            runner.run();

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Results:", 80));

            for (Iterator j=runner.getResults().iterator(); j.hasNext(); ) {
                AttributeValues av = (AttributeValues)j.next();
                log.debug(Formatter.displayLine(" - "+av, 80));
                parentResults.add(av);
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return parentResults;
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
