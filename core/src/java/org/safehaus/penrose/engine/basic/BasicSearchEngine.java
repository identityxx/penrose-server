package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.session.Session;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BasicSearchEngine {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private BasicEngine engine;

    public BasicSearchEngine(BasicEngine engine) {
        this.engine = engine;
    }

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        try {
            //List<Collection<SourceRef>> groupsOfSources = engine.getLocalGroupsOfSources(partition, entryMapping, baseMapping);
            List<Collection<SourceRef>> groupsOfSources = engine.getGroupsOfSources(partition, entryMapping);
            final Interpreter interpreter = engine.getInterpreterManager().newInstance();

            if (groupsOfSources.isEmpty()) {
                if (debug) log.debug("Returning static entry "+entryMapping.getDn());

                interpreter.set(sourceValues);
                Attributes attributes = engine.computeAttributes(interpreter, entryMapping);
                interpreter.clear();

                SearchResult searchResult = new SearchResult(entryMapping.getDn(), attributes);
                searchResult.setEntryMapping(entryMapping);
                searchResult.setSourceValues((SourceValues)sourceValues.clone());
                response.add(searchResult);

                return;
            }

            Collection<SourceRef> sourceRefs = groupsOfSources.get(0);
            Connector connector = engine.getConnector(sourceRefs.iterator().next());

            BasicSearchResponse sr = new BasicSearchResponse(
                    session,
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
                    session,
                    partition,
                    entryMapping,
                    sourceRefs,
                    sourceValues,
                    request,
                    sr
            );

        } finally {
            response.close();
        }
    }
}
