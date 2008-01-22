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

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.engine.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.FieldMapping;
import org.safehaus.penrose.directory.SourceMapping;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SimpleEngine extends Engine {

    SearchEngine searchEngine;

    public void init() throws Exception {
        super.init();

        searchEngine = new SearchEngine(this);

        log.debug("Default engine initialized.");
    }

    public SearchEngine getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    public void extractSourceValues(
            Partition partition,
            Entry entry,
            DN dn,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting source values from "+dn);

        for (Iterator i=dn.getRdns().iterator(); i.hasNext() && entry != null; ) {
            RDN rdn = (RDN)i.next();

            Collection<SourceMapping> sourceMappings = entry.getSourceMappings();
            for (SourceMapping sourceMapping : sourceMappings) {
                extractSourceValues(
                        partition,
                        rdn,
                        sourceMapping,
                        sourceValues
                );
            }

            entry = entry.getParent();
        }
    }

    public void extractSourceValues(
            Partition partition,
            RDN rdn,
            SourceMapping sourceMapping,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        Attributes attributes = sourceValues.get(sourceMapping.getName());

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SourceConfigs sources = partitionConfig.getSourceConfigs();
        SourceConfig sourceConfig = sources.getSourceConfig(sourceMapping.getSourceName());

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());

            if (fieldMapping.getVariable() == null) continue;

            String variable = fieldMapping.getVariable();
            Object value = rdn.get(variable);
            if (value == null) continue;

            if ("INTEGER".equals(fieldConfig.getType()) && value instanceof String) {
                value = Integer.parseInt((String)value);
            }

            attributes.setValue(fieldMapping.getName(), value);

            String fieldName = sourceMapping.getName() + "." + fieldMapping.getName();
            if (debug) log.debug(" => " + fieldName + ": " + value);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Entry entry,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }
/*
        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }
*/
        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> primarySources = iterator.next();

        SourceRef sourceRef = primarySources.iterator().next();
        Source source = sourceRef.getSource();

        source.add(
                session,
                primarySources,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Entry entry,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }
/*
        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }
*/
        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> primarySources = iterator.next();

        SourceRef sourceRef = primarySources.iterator().next();
        Source source = sourceRef.getSource();

        try {
            source.bind(
                    session,
                    primarySources,
                    sourceValues,
                    request,
                    response
            );

        } catch (LDAPException e) {
            if (e.getResultCode() == LDAP.INVALID_CREDENTIALS) {
                log.debug("Calling default bind operation.");
                super.bind(session, entry, sourceValues, request, response);
            } else {
                throw e;
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Entry entry,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+ entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }
/*
        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entry, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }
*/
        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> primarySources = iterator.next();

        SourceRef sourceRef = primarySources.iterator().next();
        Source source = sourceRef.getSource();

        source.delete(
                session,
                primarySources,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Entry entry,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+ entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }
/*
        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entry, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }
*/
        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> primarySources = iterator.next();

        SourceRef sourceRef = primarySources.iterator().next();
        Source source = sourceRef.getSource();

        source.modify(
                session,
                primarySources,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Entry entry,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+ entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }
/*
        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entry, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }
*/
        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();
        Collection<SourceRef> primarySources = iterator.next();

        SourceRef sourceRef = primarySources.iterator().next();
        Source source = sourceRef.getSource();

        source.modrdn(
                session,
                primarySources,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Entry base,
            Entry entry,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Base DN       : "+request.getDn(), 80));
            log.debug(Formatter.displayLine("Base Mapping  : "+base.getDn(), 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+ entry.getDn(), 80));
            log.debug(Formatter.displayLine("Filter        : "+request.getFilter(), 80));
            log.debug(Formatter.displayLine("Scope         : "+ LDAP.getScope(request.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        try {
            extractSourceValues(partition, base, request.getDn(), sourceValues);
            //EngineTool.propagateDown(partition, entry, sourceValues);

            if (debug) {
                log.debug("Source values:");
                sourceValues.print();
            }

            searchEngine.search(
                    session,
                    partition,
                    entry,
                    sourceValues,
                    request,
                    response
            );

        } finally {
            response.close();
        }
    }
}

