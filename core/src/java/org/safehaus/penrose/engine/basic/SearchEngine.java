package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.EntryUtil;

import java.util.Collection;
import java.util.Iterator;

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
            final AttributeValues sourceValues,
            final SearchRequest request,
            final SearchResponse results
    ) throws Exception {

        try {
            final boolean debug = log.isDebugEnabled();
            Collection sourceMappings = entryMapping.getSourceMappings();
            final Interpreter interpreter = engine.getInterpreterManager().newInstance();

            if (sourceMappings.size() == 0) {
                if (debug) log.debug("Returning static entry "+entryMapping.getDn());

                Attributes attributes = computeAttributes(interpreter, entryMapping, new Attributes());

                Entry entry = new Entry(entryMapping.getDn(), entryMapping, attributes);
                results.add(entry);

                return;
            }

            SearchResponse sr = new SearchResponse() {
                public void add(Object object) throws Exception {
                    Entry result = (Entry)object;
                    EntryMapping em = result.getEntryMapping();

                    Attributes sv = EntryUtil.computeAttributes(sourceValues);
                    sv.add(result.getAttributes());

                    EngineTool.propagateUp(partition, em, sv);

                    if (debug) {
                        log.debug("Source values:");
                        sv.print();
                    }

                    Collection dns = engine.computeDns(partition, interpreter, em, sv);
                    Attributes attributes = computeAttributes(interpreter, em, sv);

                    if (debug) {
                        log.debug("Attributes:");
                        attributes.print();
                    }

                    for (Iterator i=dns.iterator(); i.hasNext(); ) {
                        DN dn = (DN)i.next();

                        Entry data = new Entry(dn, em, attributes);
                        results.add(data);
                    }
                }
            };

            SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

            Connector connector = engine.getConnector(sourceConfig);

            connector.search(
                    partition,
                    entryMapping,
                    sourceMappings,
                    sourceValues,
                    request,
                    sr
            );

        } finally {
            results.close();
        }
    }

    public Attributes computeAttributes(
            Interpreter interpreter,
            EntryMapping entryMapping,
            Attributes sourceValues
    ) throws Exception {

        interpreter.set(sourceValues);

        Attributes attributes = new Attributes();

        Collection attributeMappings = entryMapping.getAttributeMappings();

        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            Object value = interpreter.eval(entryMapping, attributeMapping);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection)value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        Collection objectClasses = entryMapping.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributes.addValue("objectClass", objectClass);
        }

        interpreter.clear();

        return attributes;
    }
}
