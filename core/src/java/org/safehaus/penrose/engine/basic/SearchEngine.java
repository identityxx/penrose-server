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
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.connector.ConnectorSearchResult;
import org.safehaus.penrose.connector.Connector;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    private Engine engine;

    public SearchEngine(Engine engine) {
        this.engine = engine;
    }

    /**
     * @param partition
     * @param sourceValues
     * @param entryMapping
     * @param filter
     * @param results Collection of EntryData.
     * @throws Exception
     */
    public void search(
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping entryMapping,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
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

            SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());

            final String sourceName = sourceMapping.getName();
            String prefix = sourceName+".";

            Filter f = engine.getEngineFilterTool().toSourceFilter(partition, sourceValues, entryMapping, sourceMapping, filter);
            for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                if (!name.startsWith(prefix)) continue;

                String fieldName = name.substring(sourceName.length()+1);
                Collection values = sourceValues.get(name);
                if (values == null) continue;

                Object value = values.iterator().next();
                f = FilterTool.appendAndFilter(f, new SimpleFilter(fieldName, "=", value.toString()));
            }

            if (debug) log.debug("Source filter: "+f);

            Pipeline sr = new Pipeline(results) {
                public void add(Object object) throws Exception {
                    ConnectorSearchResult result = (ConnectorSearchResult)object;
                    EntryMapping em = result.getEntryMapping();
                    SourceMapping sm = result.getSourceMapping();

                    AttributeValues sv = new AttributeValues(sourceValues);
                    sv.set(sm.getName(), result.getSourceValues());

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

                    super.add(data);
                }
            };

            Connector connector = engine.getConnector(sourceConfig);
            connector.search(partition, entryMapping, sourceMapping, sourceConfig, null, f, sc, sr);

        } finally {
            results.close();
        }
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public DN computeDn(
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues sourceValues)
            throws Exception {

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

        while (em != null) {
            Collection rdnAttributes = em.getRdnAttributeMappings();

            for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)i.next();
                String variable = attributeMapping.getVariable();
                if (variable == null) continue; // skip static rdn

                Object value = null;

                Collection values = sourceValues.get(variable);
                if (values != null) {
                    if (values.size() >= 1) {
                        value = values.iterator().next();
                    }
                }

                args.add(value);
            }

            em = partition.getParent(em);
        }
    }
}
