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

import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.Modification;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.engine.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.Connection;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SimpleEngine extends Engine {

    SearchEngine searchEngine;

    public void init() throws Exception {
        super.init();

        engineFilterTool = new EngineFilterTool(this);
        searchEngine     = new SearchEngine(this);
        transformEngine  = new TransformEngine(this);

        log.debug("Default engine initialized.");
    }

    public void bind(Session session, Partition partition, EntryMapping entryMapping, DN dn, String password) throws Exception {

        log.debug("Bind as user "+dn);

        RDN rdn = dn.getRdn();

        AttributeValues attributeValues = new AttributeValues();
        attributeValues.add(rdn);

        Collection sources = entryMapping.getSourceMappings();

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping source = (SourceMapping)i.next();
            SourceConfig sourceConfig = partition.getSourceConfig(source.getSourceName());
            Connector connector = getConnector(sourceConfig);

            Map entries = transformEngine.split(partition, entryMapping, source, dn, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                RDN pk = (RDN)j.next();
                //AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+".");

                try {
                    connector.bind(partition, sourceConfig, entryMapping, pk, password);
                    return;
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    public void add(
            Session session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            DN dn,
            Attributes attributes
    ) throws Exception {

        AttributeValues attributeValues = EntryUtil.computeAttributeValues(attributes);
        Collection sourceMappings = entryMapping.getSourceMappings();

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());
            Connector connector = getConnector(sourceConfig);

            Map entries = transformEngine.split(partition, entryMapping, sourceMapping, dn, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                RDN pk = (RDN)j.next();
                AttributeValues sv = (AttributeValues)entries.get(pk);

                if (log.isDebugEnabled()) log.debug("Adding to "+sourceMapping.getName()+" entry "+pk+": "+sv);

                connector.add(partition, sourceConfig, sv);
            }
        }
    }

    public void delete(Session session, Partition partition, Entry entry, EntryMapping entryMapping, DN dn) throws Exception {

        RDN rdn = dn.getRdn();
        AttributeValues attributeValues = new AttributeValues();
        attributeValues.add(rdn);

        Collection sourceMappings = entryMapping.getSourceMappings();

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());
            Connector connector = getConnector(sourceConfig);

            Map entries = transformEngine.split(partition, entryMapping, sourceMapping, dn, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                RDN pk = (RDN)j.next();
                AttributeValues sv = (AttributeValues)entries.get(pk);

                log.debug("Adding to "+sourceMapping.getName()+" entry "+pk+": "+sv);

                connector.delete(partition, sourceConfig, sv);
            }
        }
    }

    public void modrdn(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        RDN rdn1 = dn.getRdn();
        AttributeValues av1 = new AttributeValues();
        av1.add(rdn1);

        RDN rdn2 = newRdn;
        AttributeValues av2 = new AttributeValues();
        av2.add(rdn2);

        Collection sourceMappings = entryMapping.getSourceMappings();
        SourceMapping primarySourceMapping = getPrimarySource(entryMapping);
        log.debug("Primary source: "+primarySourceMapping.getName());

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            log.debug("Renaming source "+sourceMapping.getName());

            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());
            Connector connector = getConnector(sourceConfig);

            Map entries1 = transformEngine.split(partition, entryMapping, sourceMapping, dn, av1);
            Map entries2 = transformEngine.split(partition, entryMapping, sourceMapping, dn, av2);

            log.debug("Entries 1: "+entries1);
            log.debug("Entries 2: "+entries2);

            RDN oldPk = (RDN)entries1.keySet().iterator().next();
            RDN newPk = (RDN)entries2.keySet().iterator().next();

            log.debug("Renaming "+newPk+" into "+newPk);

            connector.modrdn(partition, sourceConfig, oldPk, newPk, deleteOldRdn);
        }
    }

    public void modify(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            Collection modifications
    ) throws Exception {

        AttributeValues newAttributeValues = new AttributeValues();

        RDN rdn = dn.getRdn();
        AttributeValues attributeValues = new AttributeValues();
        attributeValues.add(rdn);

        for (Iterator iterator=modifications.iterator(); iterator.hasNext(); ) {
            Modification mi = (Modification)iterator.next();
            Attribute attribute = mi.getAttribute();
            String name = attribute.getName();
            attributeValues.add(name, attribute.getValues());
        }

        Collection sourceMappings = entryMapping.getSourceMappings();
        SourceMapping primarySourceMapping = getPrimarySource(entryMapping);
        if (log.isDebugEnabled()) log.debug("Primary source: "+primarySourceMapping.getName());

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            if (log.isDebugEnabled()) log.debug("Modifying source "+sourceMapping.getName());

            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());
            Connector connector = getConnector(sourceConfig);

            Map entries = transformEngine.split(partition, entryMapping, sourceMapping, dn, attributeValues);

            RDN pk = (RDN)entries.keySet().iterator().next();
            RDNBuilder rb = new RDNBuilder();
            rb.set(pk);

            boolean deleteExistingEntries = false;
            // Convert modification list of attributes into modification list of fields
            Collection mods = new ArrayList();
            for (Iterator j=sourceMapping.getFieldMappings().iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                if (fieldMapping.getVariable() == null) continue;

                String name = fieldMapping.getName();
                String variable = fieldMapping.getVariable();

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);

                for (Iterator k=modifications.iterator(); k.hasNext(); ) {
                    Modification mi = (Modification)k.next();

                    int type = mi.getType();
                    Attribute attribute = mi.getAttribute();
                    if (!attribute.getName().equals(variable)) continue;

                    if (log.isDebugEnabled()) log.debug("Converting modification for attribute "+variable);

                    Attribute newAttr = new Attribute(name, attribute.getValues());
                    mods.add(new Modification(type, newAttr));

                    if ((!sourceMapping.equals(primarySourceMapping)) && fieldConfig.isPrimaryKey()) {
                        deleteExistingEntries = true;
                        if (log.isDebugEnabled()) log.debug("Removing field "+name);
                        rb.remove(name);
                    }
                }
            }

            RDN pk2 = rb.toRdn();

            if (log.isDebugEnabled()) log.debug("PK: "+pk);
            if (log.isDebugEnabled()) log.debug("PK2: "+pk2);

            if (deleteExistingEntries) {
                Connection connection = connector.getConnection(partition, sourceConfig.getConnectionName());
                connection.delete(sourceConfig, pk2);
            }

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                pk = (RDN)j.next();
                AttributeValues sv = (AttributeValues)entries.get(pk);
                if (log.isDebugEnabled()) log.debug("Modifying entry "+pk+" in "+sourceConfig.getName()+": "+sv);

                connector.modify(partition, sourceConfig, pk, mods, sv, newAttributeValues);
            }
        }
    }

    public SearchEngine getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    public void start() throws Exception {
        super.start();

        //log.debug("Starting Engine...");

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                analyzer.analyze(partition, entryMapping);
            }
        }

        //threadManager.execute(new RefreshThread(this));

        //log.debug("Engine started.");
    }

    public void stop() throws Exception {
        if (stopping) return;

        log.debug("Stopping Engine...");
        stopping = true;

        // wait for all the worker threads to finish
        //if (threadManager != null) threadManager.stopRequestAllWorkers();
        log.debug("Engine stopped.");
        super.stop();
    }

    public Entry find(
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("DN: "+dn, 80));
            log.debug(Formatter.displayLine("Mapping: "+entryMapping.getDn(), 80));

            if (!sourceValues.isEmpty()) {
                log.debug(Formatter.displayLine("Source values:", 80));
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        RDN rdn = dn.getRdn();
        Filter filter = FilterTool.createFilter(rdn);

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(
                null,
                partition,
                sourceValues,
                entryMapping,
                request,
                response
        );

        Entry entry = null;
        if (response.hasNext() && getFilterTool().isValid(entry, filter)) {
            entry = (Entry) response.next();
        }


        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND RESULT", 80));

            if (entry == null) {
                log.debug(Formatter.displayLine("Entry \""+dn+"\" not found", 80));
            } else {
                log.debug(Formatter.displayLine(" - "+(entry == null ? null : entry.getDn()), 80));
            }

            if (!sourceValues.isEmpty()) {
                log.debug(Formatter.displayLine("Source values:", 80));
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        return entry;
    }

    public void search(
            final Session session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        final DN baseDn = request.getDn();
        final Filter filter = request.getFilter();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Mapping DN: "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(request.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        try {
            extractSourceValues(partition, baseMapping, baseDn, sourceValues);

            List mappings = new ArrayList();

            EntryMapping em = entryMapping;
            while (em != baseMapping) {
                mappings.add(0, em);
                em = partition.getParent(em);
            }

            while (em != null) {
                mappings.add(0, em);
                em = partition.getParent(em);
            }

            EngineTool.propagate(mappings, sourceValues);

            if (debug) {
                log.debug("Source values:");
                sourceValues.print();
            }

            SearchResponse sr = new SimpleEngineSearchResponse(response);

            searchEngine.search(partition, sourceValues, entryMapping, request, sr);

        } finally {
            response.close();
        }
    }

    public void extractSourceValues(Partition partition, EntryMapping entryMapping, DN dn, AttributeValues sourceValues) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Extracting source values from "+dn);

        for (Iterator i=dn.getRdns().iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            Collection sourceMappings = entryMapping.getSourceMappings();
            for (Iterator j=sourceMappings.iterator(); j.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)j.next();
                extractSourceValues(rdn, sourceMapping, sourceValues);
            }

            entryMapping = partition.getParent(entryMapping);
        }
    }

    public void extractSourceValues(RDN rdn, SourceMapping sourceMapping, AttributeValues sourceValues) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();
            //log.debug("Field: "+fieldMapping.getName());
            if (fieldMapping.getVariable() == null) continue;

            String variable = fieldMapping.getVariable();
            //log.debug("Variable: "+variable);
            Object value = rdn.get(variable);
            if (value == null) continue;

            String fieldName = sourceMapping.getName()+"."+fieldMapping.getName();
            sourceValues.set(fieldName, value);
            if (debug) log.debug(" => "+fieldName+": "+value);
        }
    }

}

