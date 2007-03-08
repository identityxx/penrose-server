/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.engine.simple;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.ConnectorSearchResult;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.engine.simple.SimpleEngine;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.engine.EntryData;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

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

                    DN dn = ((SimpleEngine)engine).computeDn(partition, em, sv);

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
}
