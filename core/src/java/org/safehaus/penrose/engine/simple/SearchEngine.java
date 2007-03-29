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
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.util.EntryUtil;
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

    public void search(
            final Partition partition,
            final EntryMapping entryMapping,
            final AttributeValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        try {
            final boolean debug = log.isDebugEnabled();
            Collection sourceMappings = entryMapping.getSourceMappings();

            if (sourceMappings.size() == 0) {
                if (debug) log.debug("Returning static entry "+entryMapping.getDn());
                
                Attributes sv = EntryUtil.computeAttributes(sourceValues);
                Attributes attributes = computeAttributes(entryMapping, sv);

                Entry entry = new Entry(entryMapping.getDn(), entryMapping, attributes);
                response.add(entry);

                return;
            }

            SearchResponse sr = new SearchResponse() {
                public void add(Object object) throws Exception {
                    Entry result = (Entry)object;

                    EntryMapping em = result.getEntryMapping();

                    Attributes sv = EntryUtil.computeAttributes(sourceValues);

                    for (Iterator i=result.getSourceNames().iterator(); i.hasNext(); ) {
                        String sourceName = (String)i.next();
                        Attributes esv = result.getSourceValues(sourceName);
                        sv.add(sourceName, esv);
                    }

                    EngineTool.propagateUp(partition, em, sv);

                    if (debug) {
                        log.debug("Source values:");
                        sv.print();
                    }

                    DN dn = computeDn(partition, em, sv);
                    Attributes attributes = computeAttributes(em, sv);

                    if (debug) {
                        log.debug("Attributes:");
                        attributes.print();
                    }

                    if (debug) log.debug("Generating entry "+dn);
                    Entry entry = new Entry(dn, em, attributes);
                    response.add(entry);
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
            response.close();
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
            Attributes sourceValues)
            throws Exception {

        Collection args = new ArrayList();
        computeArguments(partition, entryMapping, sourceValues, args);

        DN dn = entryMapping.getDn();
        DN newDn = new DN(dn.format(args));

        return newDn;
    }

    public void computeArguments(
            Partition partition,
            EntryMapping entryMapping,
            Attributes sourceValues,
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

                Collection values = sourceValues.getValues(variable);
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

    public Attributes computeAttributes(EntryMapping entryMapping, Attributes sourceValues) {

        Attributes attributes = new Attributes();

        Collection attributeMappings = entryMapping.getAttributeMappings();
        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            String name = attributeMapping.getName();

            String constant = (String)attributeMapping.getConstant();
            if (constant != null) {
                attributes.addValue(name, constant);
                continue;
            }

            String variable = attributeMapping.getVariable();
            if (variable != null) {
                Collection values = sourceValues.getValues(variable);
                attributes.addValues(name, values);
                continue;
            }
        }

        Collection objectClasses = entryMapping.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }
}
