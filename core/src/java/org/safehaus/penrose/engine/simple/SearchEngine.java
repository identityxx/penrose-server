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

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.connection.Connection;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchEngine {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private Engine engine;

    public SearchEngine(Engine engine) {
        this.engine = engine;
    }

    public void search(
            final Session session,
            final Partition partition,
            final Entry entry,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        try {
            Collection sourceMappings = entry.getEntryMapping().getSourceMappings();

            if (sourceMappings.size() == 0) {
                if (debug) log.debug("Returning static entry "+entry.getDn());
                
                Attributes attributes = computeAttributes(entry, sourceValues);

                SearchResult searchResult = new SearchResult(entry.getDn(), attributes);
                searchResult.setEntry(entry);
                response.add(searchResult);

                return;
            }

            SearchResponse sr = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    //Entry entry = result.getEntry();

                    SourceValues sv = result.getSourceValues();
                    sv.add(sourceValues);

                    EngineTool.propagateUp(entry, sv);

                    if (debug) {
                        log.debug("Source values:");
                        sv.print();
                    }

                    DN dn = computeDn(partition, entry, sv);
                    Attributes attributes = computeAttributes(entry, sv);

                    if (debug) {
                        log.debug("Attributes:");
                        attributes.print();
                    }

                    if (debug) log.debug("Generating entry "+dn);
                    SearchResult searchResult = new SearchResult(dn, attributes);
                    searchResult.setEntry(entry);
                    response.add(searchResult);
                }
            };

            Collection<Collection<SourceRef>> groupsOfSources = engine.getGroupsOfSources(partition, entry);

            Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
            Collection<SourceRef> primarySources = iterator.next();

            SourceRef sourceRef = primarySources.iterator().next();

            //Collection<SourceRef> primarySourceRefs = entry.getPrimarySourceRefs();
            Collection<SourceRef> localSourceRefs = entry.getLocalSourceRefs();

            Source source = sourceRef.getSource();

            source.search(
                    session,
                    //primarySourceRefs,
                    localSourceRefs,
                    primarySources,
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
            Entry entry,
            SourceValues sourceValues
    ) throws Exception {

        Collection<Object> args = new ArrayList<Object>();
        computeDnArguments(partition, entry, sourceValues, args);

        DN dn = entry.getDn();

        return new DN(dn.format(args));
    }

    public void computeDnArguments(
            Partition partition,
            Entry entry,
            SourceValues sourceValues,
            Collection<Object> args
    ) throws Exception {

        Entry em = entry;

        while (em != null) {
            Collection<AttributeMapping> rdnAttributes = em.getRdnAttributeMappings();

            for (AttributeMapping attributeMapping : rdnAttributes) {

                String variable = attributeMapping.getVariable();
                if (variable == null) continue; // skip static rdn

                int p = variable.indexOf(".");
                String sourceName = variable.substring(0, p);
                String fieldName = variable.substring(p + 1);

                Attributes attributes = sourceValues.get(sourceName);
                Object value = attributes.getValue(fieldName);

                args.add(value);
            }

            em = entry.getParent();
        }
    }

    public Attributes computeAttributes(Entry entry, SourceValues sourceValues) {

        Attributes attributes = new Attributes();

        Collection<AttributeMapping> attributeMappings = entry.getAttributeMappings();
        for (AttributeMapping attributeMapping : attributeMappings) {
            String name = attributeMapping.getName();

            String constant = (String) attributeMapping.getConstant();
            if (constant != null) {
                attributes.addValue(name, constant);
                continue;
            }

            String variable = attributeMapping.getVariable();
            if (variable == null) continue;

            int p = variable.indexOf(".");
            String sourceName = variable.substring(0, p);
            String fieldName = variable.substring(p + 1);

            Attributes attrs = sourceValues.get(sourceName);
            Collection<Object> values = attrs.getValues(fieldName);

            attributes.addValues(name, values);
        }

        Collection<String> objectClasses = entry.getObjectClasses();
        for (String objectClass : objectClasses) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }
}
