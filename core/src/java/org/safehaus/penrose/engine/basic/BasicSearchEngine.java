package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.SourceRef;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BasicSearchEngine {

    public Logger log = LoggerFactory.getLogger(getClass());

    private BasicEngine engine;

    public BasicSearchEngine(BasicEngine engine) {
        this.engine = engine;
    }

    public void search(
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        try {
            final boolean debug = log.isDebugEnabled();

            List<Collection<SourceRef>> groupsOfSources = engine.createGroupsOfSources(partition, entryMapping);
            final Interpreter interpreter = engine.getInterpreterManager().newInstance();

            if (groupsOfSources.isEmpty()) {
                if (debug) log.debug("Returning static entry "+entryMapping.getDn());

                interpreter.set(sourceValues);
                Attributes attributes = engine.computeAttributes(interpreter, entryMapping);
                interpreter.clear();

                SearchResult searchResult = new SearchResult(entryMapping.getDn(), attributes);
                searchResult.setEntryMapping(entryMapping);
                response.add(searchResult);

                return;
            }

            Collection<SourceRef> group = groupsOfSources.get(0);

            SourceRef sourceRef = group.iterator().next();
            Connector connector = engine.getConnector(sourceRef);

            BasicSearchResponse sr = new BasicSearchResponse(
                    partition,
                    engine,
                    entryMapping,
                    groupsOfSources,
                    sourceValues,
                    interpreter,
                    request,
                    response
            );

            connector.search(
                    partition,
                    entryMapping,
                    group,
                    sourceValues,
                    request,
                    sr
            );

        } finally {
            response.close();
        }
    }
}
