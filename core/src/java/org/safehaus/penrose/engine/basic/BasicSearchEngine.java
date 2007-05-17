package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.SourceRef;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BasicSearchEngine {

    Logger log = LoggerFactory.getLogger(getClass());

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
                Attributes attributes = computeAttributes(interpreter, entryMapping);
                interpreter.clear();

                SearchResult searchResult = new SearchResult(entryMapping.getDn(), attributes);
                searchResult.setEntryMapping(entryMapping);
                response.add(searchResult);

                return;
            }

            SearchResponse<SearchResult> entryGenerator = new SearchResponse<SearchResult>() {
                public void add(SearchResult result) throws Exception {
                    EntryMapping em = result.getEntryMapping();
                    log.debug("Generating entry "+em.getDn());

                    SourceValues sv = result.getSourceValues();
                    sv.add(sourceValues);

                    if (debug) {
                        log.debug("Source values:");
                        sv.print();
                    }

                    interpreter.set(sv);
                    Collection<DN> dns = engine.computeDns(partition, interpreter, em);
                    Attributes attributes = computeAttributes(interpreter, em);
                    interpreter.clear();

                    if (debug) {
                        log.debug("Attributes:");
                        attributes.print();
                    }

                    for (DN dn : dns) {
                        if (debug) log.debug("Generating entry " + dn);
                        SearchResult searchResult = new SearchResult(dn, attributes);
                        searchResult.setEntryMapping(em);
                        response.add(searchResult);
                    }
                }
            };

            Collection<SourceRef> primarySources = groupsOfSources.get(0);

            SourceRef sourceRef = primarySources.iterator().next();
            Connector connector = engine.getConnector(sourceRef);

            BasicSearchResponse sr = new BasicSearchResponse(
                    partition,
                    engine,
                    entryMapping,
                    groupsOfSources,
                    0,
                    sourceValues,
                    request,
                    entryGenerator
            );

            connector.search(
                    partition,
                    entryMapping,
                    primarySources,
                    sourceValues,
                    request,
                    sr
            );

        } finally {
            response.close();
        }
    }

    public Attributes computeAttributes(
            Interpreter interpreter,
            EntryMapping entryMapping
    ) throws Exception {

        Attributes attributes = new Attributes();

        Collection<AttributeMapping> attributeMappings = entryMapping.getAttributeMappings();

        for (AttributeMapping attributeMapping : attributeMappings) {

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection) value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        Collection<String> objectClasses = entryMapping.getObjectClasses();
        for (String objectClass : objectClasses) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }
}
