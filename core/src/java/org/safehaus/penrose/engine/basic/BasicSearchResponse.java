package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.engine.EngineTool;

import java.util.Collection;
import java.util.List;

/**
 * @author Endi Sukma Dewata
 */
public class BasicSearchResponse extends SearchResponse<SearchResult> {

    boolean debug = log.isDebugEnabled();

    Partition partition;
    EntryMapping entryMapping;
    SourceValues sourceValues;

    BasicEngine engine;

    List<Collection<SourceRef>> groupsOfSources;
    int index;

    SearchRequest request;
    SearchResponse<SearchResult> response;

    public BasicSearchResponse(
            Partition partition,
            BasicEngine engine,
            EntryMapping entryMapping,
            List<Collection<SourceRef>> groupsOfSources,
            int index,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        this.groupsOfSources = groupsOfSources;
        this.index = index;

        this.partition = partition;
        this.entryMapping = entryMapping;
        this.sourceValues = sourceValues;

        this.engine = engine;

        this.request = request;
        this.response = response;
    }

    public void add(SearchResult result) throws Exception {

        EntryMapping em = result.getEntryMapping();

        SourceValues sv = result.getSourceValues();
        sv.add(sourceValues);

        if (index+1 == groupsOfSources.size()) {
            response.add(result);
            return;
        }

        EngineTool.propagateDown(partition, em, sv);

        if (debug) {
            log.debug("Source values:");
            sv.print();
        }

        Collection<SourceRef> sourceRefs = groupsOfSources.get(index+1);
        SourceRef sourceRef = sourceRefs.iterator().next();
        Connector connector = engine.getConnector(sourceRef);

        BasicSearchResponse sr = new BasicSearchResponse(
                partition,
                engine,
                em,
                groupsOfSources,
                index+1,
                sv,
                request,
                response
        );

        connector.search(
                partition,
                em,
                sourceRefs,
                sv,
                request,
                sr
        );

    }

    public void close() throws Exception {
        response.close();
    }
}
