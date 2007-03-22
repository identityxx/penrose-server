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
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EntryData;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DN;
import org.ietf.ldap.LDAPException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private EngineImpl engine;

    public SearchEngine(EngineImpl engine) {
        this.engine = engine;
    }

    public void search(
            Partition partition,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final SearchResponse response
    ) throws Exception {

        log.info("Searching "+entryMapping.getDn()+" for "+filter+".");

        boolean staticEntry = engine.isStatic(partition, entryMapping);
        if (staticEntry) {
            log.debug("Returning static entries.");
            searchStatic(partition, parentSourceValues, entryMapping, filter, response);
            return;
        }

        boolean unique = engine.isUnique(partition, entryMapping);
        log.debug("Entry "+entryMapping.getDn()+" "+(unique ? "is" : "is not")+" unique.");

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
        final SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        if (unique && effectiveSources.size() == 1 && primarySourceMapping != null) {
            try {
                simpleSearch(partition, parentSourceValues, entryMapping, filter, response);

            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
            return;
        }

        try {
            searchDynamic(partition, parentSourceValues, entryMapping, filter, response);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }

    public void searchStatic(
            final Partition partition,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final SearchResponse response
    ) throws Exception {

        Interpreter interpreter = engine.getInterpreterManager().newInstance();

        Collection list = engine.computeDns(partition, interpreter, entryMapping, parentSourceValues);
        for (Iterator j=list.iterator(); j.hasNext(); ) {
            DN dn = (DN)j.next();
            log.debug("Static entry "+dn);

            EntryData map = new EntryData();
            map.setDn(dn);
            map.setMergedValues(parentSourceValues);
            response.add(map);
        }
        response.close();
    }

    public void searchDynamic(
            Partition partition,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final SearchResponse response)
            throws Exception {

        //boolean unique = engine.isUnique(entryMapping  //log.debug("Entry "+entryMapping" "+(unique ? "is" : "is not")+" unique.");

        EntryMapping parentMapping = partition.getParent(entryMapping);

        Interpreter interpreter = engine.getInterpreterManager().newInstance();

        SearchResponse values = new SearchResponse();
        searchSources(partition, parentSourceValues, entryMapping, filter, values);

        Map sourceValues = new HashMap();
        Map rows = new HashMap();

        Collection dns = new ArrayList();
        Map childDns = new HashMap();

        //log.debug("Search response for "+entryMapping.getDn()+":");
        while (values.hasNext()) {
            AttributeValues sv = (AttributeValues)values.next();
            //log.debug("==> "+sv);

            Collection list = engine.computeDns(partition, interpreter, entryMapping, sv);
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                DN dn = (DN)j.next();
                //log.debug("     - "+dn);

                dns.add(dn);

                DN parentDn = dn.getParentDn();

                AttributeValues av = (AttributeValues)sourceValues.get(dn);
                if (av == null) {
                    av = new AttributeValues();
                    sourceValues.put(dn, av);
                }
                av.add(sv);

                Collection r = (Collection)rows.get(dn);
                if (r == null) {
                    r = new ArrayList();
                    rows.put(dn, r);
                }
                r.add(sv);

                Collection c = (Collection)childDns.get(parentDn);
                if (c == null) {
                    c = new HashSet();
                    childDns.put(parentDn, c);
                }
                c.add(dn);
            }
        }

        if (parentMapping != null) {
            log.debug("Storing "+filter+" in entry filter cache:");
            for (Iterator i=childDns.keySet().iterator(); i.hasNext(); ) {
                DN parentDn = (DN)i.next();
                Collection c = (Collection)childDns.get(parentDn);

                log.debug(" - "+parentDn+":");
                for (Iterator j=c.iterator(); j.hasNext(); ) {
                    DN dn = (DN)j.next();
                    log.debug("   - DN: "+dn);
                }

            }
        }

        log.debug("Results:");
        for (Iterator i=sourceValues.keySet().iterator(); i.hasNext(); ) {
            DN dn = (DN)i.next();
            AttributeValues sv = (AttributeValues)sourceValues.get(dn);
            Collection r = (Collection)rows.get(dn);

            log.debug(" - "+dn);
            //log.debug("   sources: "+sv);
            //log.debug("   rows:");

            for (Iterator j=r.iterator(); j.hasNext(); ) {
                AttributeValues row = (AttributeValues)j.next();
                //log.debug("    - "+row);
            }

            EntryData map = new EntryData();
            map.setDn(dn);
            map.setMergedValues(sv);
            map.setRows(r);
            response.add(map);
        }

        int rc = values.getReturnCode();

        if (rc != LDAPException.SUCCESS) {
            log.debug("RC: "+rc);
        }
    }

    public void simpleSearch(
            final Partition partition,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final SearchResponse response) throws Exception {

        SearchPlanner planner = new SearchPlanner(
                engine,
                partition,
                entryMapping,
                filter,
                parentSourceValues);

        planner.run();

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SIMPLE SEARCH", 80));
            log.debug(Formatter.displaySeparator(80));
        }

        final SourceMapping sourceMapping = engine.getPrimarySource(entryMapping);

        Map filters = planner.getFilters();
        Filter newFilter = (Filter)filters.get(sourceMapping);

        String s = sourceMapping.getParameter(SourceMapping.FILTER);
        if (s != null) {
            Filter sourceFilter = FilterTool.parseFilter(s);
            newFilter = FilterTool.appendAndFilter(newFilter, sourceFilter);
        }

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

        final Interpreter interpreter = engine.getInterpreterManager().newInstance();

        SearchRequest request = new SearchRequest();
        final SearchResponse sr = new SearchResponse() {
            public void add(Object object) {
                AttributeValues av = (AttributeValues)object;

                try {
                    AttributeValues sv = new AttributeValues();
                    sv.add(parentSourceValues);
                    sv.add(sourceMapping.getName(), av);

                    Collection list = engine.computeDns(partition, interpreter, entryMapping, sv);
                    for (Iterator j=list.iterator(); j.hasNext(); ) {
                        DN dn = (DN)j.next();
                        log.debug("Generated DN: "+dn);

                        EntryData data = new EntryData();
                        data.setDn(dn);
                        data.setMergedValues(sv);
                        data.setComplete(true);
                        response.add(data);
                    }
                    
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void close() throws Exception {
                response.close();
            }
        };

        Connector connector = engine.getConnector(sourceConfig);
/*
        connector.search(
                partition,
                entryMapping,
                sourceMapping,
                sourceConfig,
                null,
                newFilter,
                request,
                sr
        );
*/
    }

    public void searchSources(
            Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final SearchResponse response)
            throws Exception {

        searchSourcesInBackground(partition, sourceValues, entryMapping, filter, response);
    }

    public void searchSourcesInBackground(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            Filter filter,
            SearchResponse response)
            throws Exception {

        SearchPlanner planner = new SearchPlanner(
                engine,
                partition,
                entryMapping,
                filter,
                sourceValues);

        planner.run();

        Collection connectingSources = planner.getConnectingSources();

        SourceMapping primarySourceMapping = engine.getPrimarySource(entryMapping);

        if (primarySourceMapping == null || entryMapping.getSourceMapping(primarySourceMapping.getName()) == null) {
            log.debug("Primary source is not local");
            SearchResponse localResponse = new SearchResponse();
            localResponse.add(sourceValues);
            localResponse.close();

            Collection parentResults = searchParent(
                    partition,
                    entryMapping,
                    planner,
                    localResponse,
                    connectingSources
            );

            response.addAll(parentResults);
            return;
        }

        SearchResponse localResponse = searchLocal(
                partition,
                entryMapping,
                sourceValues,
                planner,
                connectingSources
        );

        int rc = localResponse.getReturnCode();

        //log.debug("Size: "+localResponse.size());
/*
        if (localResponse.isEmpty()) {
            log.debug("Result is empty");
            return;
        }
*/
        Collection parentResults = searchParent(
                partition,
                entryMapping,
                planner,
                localResponse,
                connectingSources
        );

        response.addAll(parentResults);

        if (rc != LDAPException.SUCCESS) {
            log.debug("RC: "+rc);
        }

        return;
    }

    public SearchResponse searchLocal(
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            SearchPlanner planner,
            Collection connectingSources) throws Exception {

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
                partition,
                entryMapping,
                sourceValues,
                planner,
                startingSourceMapping,
                relationships);

        runner.run();

        Collection list = runner.getResults();
        log.debug("Got "+list.size()+" entries");

        int rc = runner.getReturnCode();
/*
        if (rc != LDAPException.SUCCESS) {
            log.debug("RC: "+rc);
            SearchResponse response = new SearchResponse();
            response.setReturnCode(rc);
            response.close();
            return response;
        }
*/
        SearchCleaner cleaner = new SearchCleaner(
                engine,
                partition,
                entryMapping,
                planner,
                primarySourceMapping);

        for (Iterator i=connectingSources.iterator(); i.hasNext(); ) {
            Map m = (Map)i.next();
            SourceMapping sourceMapping = (SourceMapping)m.get("toSource");
            cleaner.run(sourceMapping);
        }

        cleaner.clean(list);
/*
        log.debug("Search local response:");

        int counter = 1;
        for (Iterator i=list.iterator(); i.hasNext(); counter++) {
            AttributeValues av = (AttributeValues)i.next();
            log.debug("Result #"+counter);
            for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Collection v = av.get(name);
                log.debug(" - "+name+": "+v);
            }
        }
*/
        SearchResponse response = new SearchResponse();
        response.addAll(list);
        response.close();

        return response;
    }

    public Collection searchParent(
            Partition partition,
            EntryMapping entryMapping,
            SearchPlanner planner,
            SearchResponse localResponse,
            Collection startingSources) throws Exception {

        Collection results = new ArrayList();

        while (localResponse.hasNext()) {
            results.add(localResponse.next());
        }

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=results.iterator(); i.hasNext(); ) {
            AttributeValues sv = (AttributeValues)i.next();
            sourceValues.add(sv);
        }

        if (log.isDebugEnabled()) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH PARENT", 80));

            log.debug(Formatter.displayLine("Local source values:", 80));

            int counter = 1;
            for (Iterator i=results.iterator(); i.hasNext() && counter<=20; counter++) {
                AttributeValues sv = (AttributeValues)i.next();

                log.debug(Formatter.displayLine("Record #"+counter, 80));
                for (Iterator j=sv.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = sv.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

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
            log.debug("No connecting sources");

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

            if (parentMapping == null && results.size() == 0) {
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
                    partition,
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

            int counter = 1;
            for (Iterator j=results.iterator(); j.hasNext() && counter<=20; counter++) {
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

    public void setEngine(EngineImpl engine) {
        this.engine = engine;
    }
}
