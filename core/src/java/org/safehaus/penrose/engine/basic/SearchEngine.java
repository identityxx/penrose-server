package org.safehaus.penrose.engine.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EntryData;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.connector.ConnectorSearchResult;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private BasicEngine engine;

    public SearchEngine(BasicEngine engine) {
        this.engine = engine;
    }

    /**
     * @param partition
     * @param sourceValues
     * @param entryMapping
     * @param results Collection of EntryData.
     * @throws Exception
     */
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

            if (sourceMappings.size() == 0) {
                if (debug) log.debug("Returning static entry "+entryMapping.getDn());

                EntryData data = new EntryData();
                data.setDn(entryMapping.getDn());
                data.setEntryMapping(entryMapping);
                data.setMergedValues(new AttributeValues());

                results.add(data);
                return;
            }

            SearchResponse sr = new SearchResponse() {
                public void add(Object object) throws Exception {
                    ConnectorSearchResult result = (ConnectorSearchResult)object;
                    EntryMapping em = result.getEntryMapping();

                    AttributeValues sv = new AttributeValues(sourceValues);
                    sv.set(result.getSourceValues());

                    EngineTool.propagateUp(partition, em, sv);

                    if (debug) {
                        log.debug("Source values:");
                        sv.print();
                    }

                    DN dn = computeDn(partition, em, sv);

                    EntryData data = new EntryData();
                    data.setDn(dn);
                    data.setEntryMapping(em);
                    data.setMergedValues(sv);

                    results.add(data);
                }
            };

            DN baseDn = request.getDn();

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

    public BasicEngine getEngine() {
        return engine;
    }

    public void setEngine(BasicEngine engine) {
        this.engine = engine;
    }

    public DN computeDn(
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues sourceValues
    ) throws Exception {

        //boolean debug = log.isDebugEnabled();

        Collection args = new ArrayList();
        computeArguments(partition, entryMapping, sourceValues, args);

        DN dn = entryMapping.getDn();
/*
        if (debug) {
            log.debug("Mapping DN: "+dn);
            log.debug("Pattern: "+dn.getPattern());
            log.debug("Arguments:");
            for (Iterator i=args.iterator(); i.hasNext(); ) {
                log.debug(" - "+i.next());
            }
        }
*/
        DN newDn = new DN(dn.format(args));

        //if (debug) log.debug("New DN: "+newDn);

        return newDn;
    }

    public void computeArguments(
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues sourceValues,
            Collection args
    ) throws Exception {

        EntryMapping em = entryMapping;

        Interpreter interpreter = engine.getInterpreterManager().newInstance();
        interpreter.set(sourceValues);

        while (em != null) {
            Collection rdnAttributes = em.getRdnAttributeMappings();

            for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)i.next();

                Object value = interpreter.eval(em, attributeMapping);
                if (value instanceof Collection) {
                    Collection c = (Collection)value;
                    value = c.iterator().next();
                }
                
                args.add(value);
            }

            em = partition.getParent(em);
        }
    }
}
