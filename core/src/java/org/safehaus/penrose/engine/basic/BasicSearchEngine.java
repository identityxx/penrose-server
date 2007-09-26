package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.connection.Connection;

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
            final Entry base,
            final Entry entry,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        try {
            //List<Collection<SourceRef>> groupsOfSources = engine.getLocalGroupsOfSources(partition, entry, base);
            List<Collection<SourceRef>> groupsOfSources = engine.getGroupsOfSources(partition, entry);
            final Interpreter interpreter = partition.newInterpreter();

            if (groupsOfSources.isEmpty()) {
                if (debug) log.debug("Returning static entry "+ entry.getDn());

                interpreter.set(sourceValues);
                Attributes attributes = engine.computeAttributes(interpreter, entry);
                interpreter.clear();

                SearchResult searchResult = new SearchResult(entry.getDn(), attributes);
                searchResult.setEntry(entry);
                searchResult.setSourceValues((SourceValues)sourceValues.clone());
                response.add(searchResult);

                return;
            }

            Collection<SourceRef> sourceRefs = groupsOfSources.get(0);

            BasicSearchResponse sr = new BasicSearchResponse(
                    session,
                    partition,
                    engine,
                    entry,
                    groupsOfSources,
                    sourceValues,
                    interpreter,
                    request,
                    response
            );

            Collection<SourceRef> primarySourceRefs = entry.getPrimarySourceRefs();
            Collection<SourceRef> localSourceRefs = entry.getLocalSourceRefs();

            SourceRef sourceRef = sourceRefs.iterator().next();
            Source source = sourceRef.getSource();
            Connection connection = source.getConnection();

            connection.search(
                    session,
                    primarySourceRefs,
                    localSourceRefs,
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
