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
import org.safehaus.penrose.SearchResults;
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

    /**
     * Check whether all rdn attributes are either constant or unique variable, or use all primary keys from one source.
     */
    public boolean isRdnUnique(EntryDefinition entryDefinition) throws Exception {

        Collection sources = new TreeSet();
        Collection fields = new TreeSet();

        Collection attributeDefinitions = entryDefinition.getRdnAttributes();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            if (attributeDefinition.getConstant() != null) continue;

            String variable = attributeDefinition.getVariable();

            if (variable != null) { // get the source and field name
                int j = variable.indexOf(".");
                String sourceAlias = variable.substring(0, j);
                String fieldName = variable.substring(j+1);

                sources.add(sourceAlias);
                fields.add(fieldName);

                continue;
            }

            // attribute is an expression
            return false;
        }

        //log.debug("RDN sources: "+sources);

        // rdn is constant
        if (sources.isEmpty()) return true;

        // rdn uses more than one source
        if (sources.size() > 1) return false;

        String sourceAlias = (String)sources.iterator().next();
        Source source = entryDefinition.getSource(sourceAlias);

        Config config = engineContext.getConfig(entryDefinition.getDn());
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection uniqueFields = new TreeSet();
        Collection pkFields = new TreeSet();

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();
            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(fieldName);

            if (fieldDefinition.isUnique()) {
                uniqueFields.add(fieldName);
                continue;
            }

            if (fieldDefinition.isPrimaryKey()) {
                pkFields.add(fieldName);
                continue;
            }

            return false;
        }

        //log.debug("RDN unique fields: "+uniqueFields);
        //log.debug("RDN PK fields: "+pkFields);

        // rdn uses unique fields
        if (pkFields.isEmpty() && !uniqueFields.isEmpty()) return true;

        Collection list = new TreeSet();
        for (Iterator i=sourceDefinition.getFieldDefinitions().iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (!fieldDefinition.isPrimaryKey()) continue;
            list.add(fieldDefinition.getName());
        }

        //log.debug("Source PK fields: "+list);

        // rdn uses primary key fields
        return pkFields.equals(list);
    }

    public void search(
            Collection path,
            EntryDefinition entryDefinition,
            Filter filter,
            SearchResults entries)
            throws Exception {

        Map results = new TreeMap();

        boolean unique = false; //isRdnUnique(entryDefinition);

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH", 80));
        log.debug(Formatter.displayLine("Entry: "+entryDefinition.getDn(), 80));
        log.debug(Formatter.displayLine("RDN Unique: "+unique, 80));
        log.debug(Formatter.displayLine("Filter: "+filter, 80));
        log.debug(Formatter.displayLine("Parents:", 80));

        AttributeValues sourceValues = new AttributeValues();
        String prefix = null;

        for (Iterator iterator = path.iterator(); iterator.hasNext(); ) {
            Entry entry = (Entry)iterator.next();
            if (entry == null) continue;

            log.debug(Formatter.displayLine(" - "+entry.getDn(), 80));

            prefix = prefix == null ? "parent." : "parent."+prefix;

            AttributeValues av = entry.getAttributeValues();
            for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = av.get(name);
                name = prefix+name;
                log.debug(Formatter.displayLine("   - "+name+": "+values, 80));
                sourceValues.add(name, values);
            }

            AttributeValues sv = entry.getSourceValues();
            for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = sv.get(name);
                if (name.startsWith("parent.")) name = prefix+name;
                log.debug(Formatter.displayLine("   - "+name+": "+values, 80));
                sourceValues.add(name, values);
            }
        }

        log.debug(Formatter.displaySeparator(80));

        Collection dns = new TreeSet();

        if (path.size() > 0) {
            Entry parent = (Entry)path.iterator().next();
            if (parent != null) {
                String parentDn = parent.getDn();

                log.debug("Checking "+filter+" in entry filter cache for "+parentDn);
                Collection list = engineContext.getEntryFilterCache(parentDn, entryDefinition).get(filter);
                if (list != null) dns.addAll(list);
            }
        }

        if (dns.isEmpty()) {
            log.debug("Filter cache does not contain "+filter);

            Collection values = searchEntries(sourceValues, entryDefinition, filter);

            Map childDns = new HashMap();

            log.debug("Search results:");
            for (Iterator i=values.iterator(); i.hasNext(); ) {
                AttributeValues sv = (AttributeValues)i.next();

                Collection list = engine.computeDns(entryDefinition, sv);
                for (Iterator j=list.iterator(); j.hasNext(); ) {
                    String dn = (String)j.next();
                    log.debug(" - "+dn);

                    Row rdn = Entry.getRdn(dn);
                    String parentDn = Entry.getParentDn(dn);

                    //log.debug("    Getting "+rdn+" from entry source cache for "+parentDn);
                    AttributeValues oldSv = (AttributeValues)engineContext.getEntrySourceCache(parentDn, entryDefinition).get(rdn);

                    if (oldSv == null) {
                        //log.debug("   Storing "+rdn+" in entry source cache.");
                        engineContext.getEntrySourceCache(parentDn, entryDefinition).put(rdn, sv);
                    } else {
                        //log.debug("   Adding "+rdn+" in entry source cache.");
                        oldSv.add(sv);
                    }
                    //log.debug("   source values1: "+oldSv);

                    if (unique && !dns.contains(dn)) {
                        Entry entry = new Entry(dn, entryDefinition, sv, new AttributeValues());
                        entries.add(entry);
                        dns.add(dn);
                    }

                    AttributeValues av = (AttributeValues)results.get(dn);
                    if (av == null) {
                        av = new AttributeValues();
                        results.put(dn, av);
                    }
                    av.add(sv);
                    //log.debug("   source values2: "+av);

                    Collection c = (Collection)childDns.get(parentDn);
                    if (c == null) {
                        c = new TreeSet();
                        childDns.put(parentDn, c);
                    }
                    c.add(dn);
                }
            }

            log.debug("Storing "+filter+" in entry filter cache:");
            for (Iterator i=childDns.keySet().iterator(); i.hasNext(); ) {
                String parentDn = (String)i.next();
                Collection c = (Collection)childDns.get(parentDn);

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

                Row rdn = Entry.getRdn(dn);
                String parentDn = Entry.getParentDn(dn);

                log.debug("Getting "+rdn+" from entry source cache for "+parentDn);
                AttributeValues sv = (AttributeValues)engineContext.getEntrySourceCache(parentDn, entryDefinition).get(rdn);
                //log.debug("Entry source cache: "+sv);

                if (unique) {
                    Entry entry = new Entry(dn, entryDefinition, sv, new AttributeValues());
                    entries.add(entry);
                }

                AttributeValues av = (AttributeValues)results.get(dn);
                if (av == null) {
                    av = new AttributeValues();
                    results.put(dn, av);
                }
                av.add(sv);
            }
        }

        if (!unique) {
            for (Iterator i=results.keySet().iterator(); i.hasNext(); ) {
                String dn = (String)i.next();
                AttributeValues sv = (AttributeValues)results.get(dn);

                Entry entry = new Entry(dn, entryDefinition, sv, new AttributeValues());
                entries.add(entry);
            }
        }

        entries.close();
    }

    public Collection searchEntries(
            AttributeValues sourceValues,
            EntryDefinition entryDefinition,
            Filter filter)
            throws Exception {

        Source primarySource = engine.getPrimarySource(entryDefinition);

        if (primarySource == null || entryDefinition.getSource(primarySource.getName()) == null) {
            log.debug("Entry "+entryDefinition.getDn()+" doesn't have any sources.");
            Collection parentSourceValues = new ArrayList();
            parentSourceValues.add(sourceValues);
            return parentSourceValues;
        }

        SearchPlanner planner = new SearchPlanner(
                engine,
                entryDefinition,
                filter,
                sourceValues);

        planner.run();

        Collection connectingSources = planner.getConnectingSources();

        Collection results = searchLocal(
                entryDefinition,
                sourceValues,
                planner,
                connectingSources
        );

        if (results.isEmpty()) return results;

        Collection parentResults = searchParent(
                entryDefinition,
                planner,
                results,
                connectingSources
        );

        //Collection values = engine.getJoinEngine().leftJoin(results, parentResults, connectingRelationships);
/*
        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Results:", 80));

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
        return results;
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

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("SEARCH LOCAL RESULTS", 80));

        int counter = 1;
        for (Iterator i=results.iterator(); i.hasNext(); counter++) {
            AttributeValues av = (AttributeValues)i.next();
            log.debug(Formatter.displayLine("Result #"+counter, 80));
            for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection v = av.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+v, 80));
            }
        }

        log.debug(Formatter.displaySeparator(80));

        return results;
    }

    public Collection searchParent(
            EntryDefinition entryDefinition,
            SearchPlanner planner,
            Collection localResults,
            Collection startingSources) throws Exception {

        Collection results = new ArrayList();

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=localResults.iterator(); i.hasNext(); ) {
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

        for (Iterator i=startingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();

            Source fromSource = (Source)m.get("fromSource");
            Source toSource = (Source)m.get("toSource");
            Collection relationships = (Collection)m.get("relationships");

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH PARENT", 80));
            log.debug(Formatter.displayLine("Local source values:", 80));

            for (Iterator j=sourceValues.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection values = sourceValues.get(name);
                log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
            }

            log.debug(Formatter.displayLine("Starting source: "+fromSource.getName(), 80));
            log.debug(Formatter.displayLine("Relationships:", 80));

            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();
                log.debug(Formatter.displayLine(" - "+relationship, 80));
            }

            Filter filter = (Filter)filters.get(fromSource);
            log.debug(Formatter.displayLine("Filter: "+filter, 80));

            log.debug(Formatter.displaySeparator(80));

            SearchParentRunner runner = new SearchParentRunner(
                    engine,
                    entryDefinition,
                    localResults,
                    sourceValues,
                    planner,
                    fromSource,
                    relationships);

            runner.run();

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH PARENT RESULTS", 80));

            int counter = 1;
            for (Iterator j=runner.getResults().iterator(); j.hasNext(); counter++) {
                AttributeValues av = (AttributeValues)j.next();
                log.debug(Formatter.displayLine("Result #"+counter, 80));
                results.add(av);

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

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
    }
}
