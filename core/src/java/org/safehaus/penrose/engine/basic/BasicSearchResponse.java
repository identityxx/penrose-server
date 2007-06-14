package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.connector.Connector;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi Sukma Dewata
 */
public class BasicSearchResponse extends SearchResponse<SearchResult> {

    boolean debug = log.isDebugEnabled();

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

    public BasicSearchResponse(
            Partition partition,
            BasicEngine engine,
            EntryMapping entryMapping,
            List<Collection<SourceRef>> groupsOfSources,
            SourceValues sourceValues,
            Interpreter interpreter,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) {
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

    public void add(SearchResult result) throws Exception {

        EntryMapping em = result.getEntryMapping();

        SourceValues sv = result.getSourceValues();
        sv.add(sourceValues);

        EngineTool.propagateDown(partition, em, sv);

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

            } else if (lastDn.equals(dn)) {
                if (debug) log.debug("Merging entry " + dn);
                lastAttributes.add(attributes);

            } else {
                if (debug) log.debug("Returning entry " + lastDn);
                SearchResult searchResult = new SearchResult(lastDn, lastAttributes);
                searchResult.setEntryMapping(lastEntryMapping);
                response.add(searchResult);

                if (debug) log.debug("Generating entry "+dn);
                lastDn = dn;
                lastAttributes = attributes;
                lastEntryMapping = em;
            }
        }
    }

    public boolean searchSecondarySources(
            EntryMapping em,
            SourceValues sv
    ) throws Exception {
        if (groupsOfSources.size() <= 1) return true;

        for (int i=1; i<groupsOfSources.size(); i++) {
            Collection<SourceRef> group = groupsOfSources.get(1);

            SourceRef sourceRef = group.iterator().next();
            Connector connector = engine.getConnector(sourceRef);

            String flag = sourceRef.getSearch();
            if (debug) log.debug("Flag: "+flag);

            SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>();

            connector.search(
                    partition,
                    em,
                    group,
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

            EngineTool.propagateDown(partition, em, sv);

            if (debug) {
                log.debug("Source values:");
                sv.print();
            }
        }

        return true;
    }

    public void close() throws Exception {

        boolean debug = log.isDebugEnabled();

        if (lastDn != null) {
            if (debug) log.debug("Returning entry " + lastDn);
            SearchResult searchResult = new SearchResult(lastDn, lastAttributes);
            searchResult.setEntryMapping(lastEntryMapping);
            response.add(searchResult);
        }

        response.close();
    }
}
