package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.session.Session;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class BasicSearchResponse extends SearchResponse<SearchResult> {

    Session session;
    Partition partition;
    BasicEngine engine;

    EntryMapping entryMapping;
    List<Collection<SourceRef>> groupsOfSources;
    SourceValues sourceValues;
    Interpreter interpreter;

    SearchRequest request;
    SearchResponse<SearchResult> response;

    DN lastDn;
    Attributes lastAttributes;
    EntryMapping lastEntryMapping;
    SourceValues lastSourceValues;

    Map<String,Collection<EntryMapping>> paths = new HashMap<String,Collection<EntryMapping>>();

    public BasicSearchResponse(
            Session session,
            Partition partition,
            BasicEngine engine,
            EntryMapping entryMapping,
            List<Collection<SourceRef>> groupsOfSources,
            SourceValues sourceValues,
            Interpreter interpreter,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) {
        this.session = session;
        this.partition = partition;
        this.engine = engine;

        this.entryMapping = entryMapping;
        this.groupsOfSources = groupsOfSources;
        this.sourceValues = sourceValues;
        this.interpreter = interpreter;

        this.request = request;
        this.response = response;

        Collection<SourceRef> group = groupsOfSources.get(0);

        if (debug) {
            log.debug("Search group:");
            for (SourceRef sr : group) {
                Source source = sr.getSource();
                Connection connection = source.getConnection();
                Adapter adapter = connection.getAdapter();
                log.debug(" - "+sr.getAlias()+" ("+adapter.getClass().getName()+")");
            }
        }
    }

    public Collection<EntryMapping> createPath(EntryMapping entryMapping) {
        List<EntryMapping> path = new ArrayList<EntryMapping>();

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        while (entryMapping != null) {
            path.add(0, entryMapping);
            entryMapping = partitionConfig.getMappings().getParent(entryMapping);
        }

        return path;
    }

    public void add(SearchResult result) throws Exception {

        EntryMapping em = result.getEntryMapping();

        Collection<EntryMapping> path = paths.get(em.getId());
        if (path == null) {
            path = createPath(em);
            paths.put(em.getId(), path);
        }

        SourceValues sv = (SourceValues)sourceValues.clone();
        sv.set(result.getSourceValues());

        if (debug) {
            log.debug("Source values:");
            sv.print();
        }

        interpreter.set(sv);
        EngineTool.propagate(path, sv, interpreter);
        interpreter.clear();

        if (debug) {
            log.debug("Source values:");
            sv.print();
        }

        boolean complete = searchSecondarySources(em, sv);

        if (!complete) return;

        interpreter.set(sv);
        Collection<DN> dns = engine.computeDns(partition, interpreter, em);
        Attributes attributes = engine.computeAttributes(interpreter, em);
        interpreter.clear();

        if (debug) {
            log.debug("Attributes:");
            attributes.print();
        }

        for (DN dn : dns) {
            if (lastDn == null) {
                if (debug) log.debug("Generating entry "+dn);
                lastDn = dn;
                lastAttributes = attributes;
                lastEntryMapping = em;
                lastSourceValues = sv;

            } else if (lastDn.equals(dn)) {
                if (debug) log.debug("Merging entry " + dn);
                lastAttributes.add(attributes);
                lastSourceValues.add(sv);

            } else {
                if (debug) log.debug("Returning entry " + lastDn);
                SearchResult searchResult = new SearchResult(lastDn, lastAttributes);
                searchResult.setEntryMapping(lastEntryMapping);
                searchResult.setSourceValues(lastSourceValues);
                response.add(searchResult);

                if (debug) log.debug("Generating entry "+dn);
                lastDn = dn;
                lastAttributes = attributes;
                lastEntryMapping = em;
                lastSourceValues = sv;
            }
        }
    }

    public void close() throws Exception {

        if (lastDn != null) {
            if (debug) log.debug("Returning entry " + lastDn);
            SearchResult searchResult = new SearchResult(lastDn, lastAttributes);
            searchResult.setEntryMapping(lastEntryMapping);
            searchResult.setSourceValues(lastSourceValues);
            response.add(searchResult);
        }

        response.close();
    }

    public boolean searchSecondarySources(
            EntryMapping em,
            SourceValues sv
    ) throws Exception {
        if (groupsOfSources.size() <= 1) return true;

        for (int i=1; i<groupsOfSources.size(); i++) {
            Collection<SourceRef> sourceRefs = groupsOfSources.get(i);
            if (debug) log.debug("Processing group " + sourceRefs);

            SourceRef sourceRef = sourceRefs.iterator().next();
            Connector connector = engine.getConnector(sourceRef);

            String flag = sourceRef.getSearch();
            if (debug) log.debug("Flag: "+flag);

            SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>();

            connector.search(
                    session,
                    partition,
                    em,
                    sourceRefs,
                    sv,
                    request,
                    sr
            );

            if (SourceMapping.REQUIRED.equals(flag) && !sr.hasNext()) {
                log.debug("Required entries not found.");
                return false;
            }

            while (sr.hasNext()) {
                SearchResult result = sr.next();
                sv.add(result.getSourceValues());
            }

            interpreter.set(sv);
            EngineTool.propagateDown(partition, em, sv, interpreter);
            interpreter.clear();

            if (debug) {
                log.debug("Source values:");
                sv.print();
            }
        }

        return true;
    }
}
