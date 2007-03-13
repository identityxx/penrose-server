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

import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.*;
import org.ietf.ldap.LDAPException;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SimpleEngine extends Engine {

    AddEngine addEngine;
    DeleteEngine deleteEngine;
    ModifyEngine modifyEngine;
    ModRdnEngine modrdnEngine;
    SearchEngine searchEngine;

    public void init() throws Exception {
        super.init();

        engineFilterTool = new EngineFilterTool(this);
        addEngine        = new AddEngine(this);
        deleteEngine     = new DeleteEngine(this);
        modifyEngine     = new ModifyEngine(this);
        modrdnEngine     = new ModRdnEngine(this);
        searchEngine     = new SearchEngine(this);
        transformEngine  = new TransformEngine(this);

        log.debug("Default engine initialized.");
    }

    public void bind(PenroseSession session, Partition partition, EntryMapping entryMapping, DN dn, String password) throws Exception {

        log.debug("Bind as user "+dn);

        RDN rdn = dn.getRdn();

        AttributeValues attributeValues = new AttributeValues();
        attributeValues.add(rdn);

        Collection sources = entryMapping.getSourceMappings();

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping source = (SourceMapping)i.next();

            SourceConfig sourceConfig = partition.getSourceConfig(source.getSourceName());

            Map entries = transformEngine.split(partition, entryMapping, source, dn, attributeValues);

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                RDN pk = (RDN)j.next();
                //AttributeValues sourceValues = (AttributeValues)entries.get(pk);

                log.debug("Bind to "+source.getName()+" as "+pk+".");

                try {
                    getConnector(sourceConfig).bind(partition, sourceConfig, entryMapping, pk, password);
                    return;
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    public void add(
            PenroseSession session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            DN dn,
            Attributes attributes
    ) throws Exception {

        // normalize attribute names
        AttributeValues attributeValues = new AttributeValues();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();

            if ("objectClass".equalsIgnoreCase(name)) continue;

            AttributeMapping attributeMapping = entryMapping.getAttributeMapping(name);
            if (attributeMapping == null) {
                log.debug("Undefined attribute "+name);
                throw ExceptionUtil.createLDAPException(LDAPException.OBJECT_CLASS_VIOLATION);
            }

            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                attributeValues.add(name, value);
            }
        }

        addEngine.add(partition, parent, entryMapping, dn, attributeValues);
    }

    public void delete(PenroseSession session, Partition partition, Entry entry, EntryMapping entryMapping, DN dn) throws Exception {

        deleteEngine.delete(partition, entry, entryMapping, dn);
    }

    public void modrdn(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        modrdnEngine.modrdn(partition, entry, entryMapping, dn, newRdn, deleteOldRdn);
    }

    public void modify(
            PenroseSession session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DN dn,
            Collection modifications
    ) throws Exception {

        AttributeValues newAttributeValues = new AttributeValues();
        modifyEngine.modify(partition, entry, entryMapping, dn, modifications, newAttributeValues);
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

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls sc = new PenroseSearchControls();
        sc.setScope(PenroseSearchControls.SCOPE_BASE);

        RDN rdn = dn.getRdn();
        Filter filter = FilterTool.createFilter(rdn);

        search(
                null,
                partition,
                sourceValues,
                entryMapping,
                dn,
                filter,
                sc,
                results
        );

        Entry entry = null;
        if (results.hasNext() && getFilterTool().isValid(entry, filter)) {
            entry = (Entry)results.next();
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
            final PenroseSession session,
            final Partition partition,
            final AttributeValues sourceValues,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final DN baseDn,
            final Filter filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("EXPAND MAPPING", 80));
            log.debug(Formatter.displayLine("Mapping DN: "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+LDAPUtil.getScope(sc.getScope()), 80));
            log.debug(Formatter.displayLine("Parent source values:", 80));

            if (sourceValues != null) {
                for (Iterator i = sourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = sourceValues.get(name);
                    log.debug(Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(Formatter.displaySeparator(80));
        }

        try {
            DN dn = entryMapping.getDn();

            if (sc.getScope() == PenroseSearchControls.SCOPE_BASE) {
                if (!dn.matches(baseDn)) {
                    //log.debug(dn+" doesn't match "+baseDn);
                    return;
                }

            } else if (sc.getScope() == PenroseSearchControls.SCOPE_ONE) {
                if (!dn.getParentDn().matches(baseDn)) {
                    //log.debug(dn+" is not a child of "+baseDn);
                    return;
                }

            } else { // if (sc.getScope() == PenroseSearchControls.SCOPE_SUB) {
                if (!dn.endsWith(baseDn)) {
                    //log.debug(dn+" doesn't end with "+baseDn);
                    return;
                }
            }

            //log.debug("Base DN: "+baseDn);
            //log.debug("Base Mapping: "+baseMapping.getDn());

            List mappings = new ArrayList();
            
            EntryMapping em = entryMapping;
            while (em != baseMapping) {
                mappings.add(em);
                em = partition.getParent(em);
            }
            int count = mappings.size();
            
            while (em != null) {
                mappings.add(em);
                em = partition.getParent(em);
            }

            for (int i=mappings.size()-1; i>=0; i--) {
                em = (EntryMapping)mappings.get(i);
                //log.debug("Mapping: "+em.getDn());

                if (i >= count) {
                    RDN rdn = baseDn.get(i - count);
                    //log.debug("RDN: "+rdn);

                    Collection sourceMappings = em.getSourceMappings();
                    for (Iterator j=sourceMappings.iterator(); j.hasNext(); ) {
                        SourceMapping sourceMapping = (SourceMapping)j.next();

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
                            //log.debug(" => "+fieldName+": "+value);
                        }
                    }
                }

                Collection relationships = em.getRelationships();
                for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                    Relationship relationship = (Relationship)j.next();

                    String lhs = relationship.getLhs();
                    String rhs = relationship.getRhs();

                    Collection values = sourceValues.get(lhs);
                    if (values == null) {
                        values = sourceValues.get(rhs);
                        if (values != null) {
                            sourceValues.set(lhs, values);
                            //log.debug(" => "+lhs+": "+values);
                        }
                    } else {
                        sourceValues.set(rhs, values);
                        //log.debug(" => "+rhs+": "+values);
                    }
                }
            }

            if (debug) {
                log.debug("Source values:");
                sourceValues.print();
            }

            Pipeline sr = new Pipeline(results) {
                public void add(Object object) throws Exception {
                    EntryData data = (EntryData)object;

                    DN dn = data.getDn();
                    EntryMapping em = data.getEntryMapping();
                    //log.debug("Converting "+dn+" EntryData to Entry");
                    
                    AttributeValues sv = data.getMergedValues();
                    AttributeValues attributeValues = computeAttributeValues(em, sv);

                    if (debug) {
                        log.debug("Attribute values:");
                        attributeValues.print();
                    }

                    Entry entry = new Entry(dn, em, attributeValues, sv);
                    super.add(entry);
                }
            };

            //log.debug("Searching "+entryMapping.getDn());
            searchEngine.search(partition, sourceValues, entryMapping, filter, sc, sr);

        } finally {
            results.close();
        }
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

    public AttributeValues computeAttributeValues(
            EntryMapping entryMapping,
            AttributeValues sourceValues
            ) throws Exception {

        AttributeValues attributeValues = new AttributeValues();

        Collection attributeMappings = entryMapping.getAttributeMappings();

        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            String name = attributeMapping.getName();

            String constant = (String)attributeMapping.getConstant();
            if (constant != null) {
                attributeValues.add(name, constant);
                continue;
            }

            String variable = attributeMapping.getVariable();
            if (variable != null) {
                Collection values = sourceValues.get(variable);
                attributeValues.add(name, values);
                continue;
            }
        }

        Collection objectClasses = entryMapping.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributeValues.add("objectClass", objectClass);
        }

        return attributeValues;
    }
}

