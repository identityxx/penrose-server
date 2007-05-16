package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.source.SourceRef;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private BasicEngine engine;

    public SearchEngine(BasicEngine engine) {
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
            Collection sourceMappings = entryMapping.getSourceMappings();
            final Interpreter interpreter = engine.getInterpreterManager().newInstance();

            if (sourceMappings.size() == 0) {
                if (debug) log.debug("Returning static entry "+entryMapping.getDn());

                Attributes sv = EntryUtil.computeAttributes(sourceValues);

                interpreter.set(sv);
                Attributes attributes = computeAttributes(interpreter, entryMapping, sv);
                interpreter.clear();

                SearchResult searchResult = new SearchResult(entryMapping.getDn(), attributes);
                searchResult.setEntryMapping(entryMapping);
                response.add(searchResult);

                return;
            }

            SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>() {
                public void add(SearchResult result) throws Exception {
                    EntryMapping em = result.getEntryMapping();

                    Attributes sv = EntryUtil.computeAttributes(sourceValues);

                    for (String sourceName : result.getSourceNames()) {
                        Attributes esv = result.getSourceAttributes(sourceName);
                        sv.add(sourceName, esv);
                    }

                    EngineTool.propagateUp(partition, em, sv);

                    if (debug) {
                        log.debug("Source values:");
                        sv.print();
                    }

                    interpreter.set(sv);

                    Collection<DN> dns = engine.computeDns(partition, interpreter, em, sv);
                    Attributes attributes = computeAttributes(interpreter, em, sv);

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

            Collection<Collection<SourceRef>> groupsOfSources = engine.createGroupsOfSources(partition, entryMapping);

            Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
            Collection<SourceRef> primarySources = iterator.next();

            SourceRef sourceRef = primarySources.iterator().next();
            Connector connector = engine.getConnector(sourceRef);

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
            EntryMapping entryMapping,
            Attributes sourceValues
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
