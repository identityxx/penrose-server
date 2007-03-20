package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineFilterTool;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BasicEngine extends Engine {

    SearchEngine searchEngine;

    public void init() throws Exception {
        super.init();

        engineFilterTool = new EngineFilterTool(this);
        searchEngine     = new SearchEngine(this);
        transformEngine  = new TransformEngine(this);

        log.debug("Default engine initialized.");
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

    public void extractSourceValues(
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            AttributeValues sourceValues
    ) throws Exception {

        Interpreter interpreter = interpreterManager.newInstance();

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Extracting source values from "+dn);

        for (Iterator i=dn.getRdns().iterator(); i.hasNext() && entryMapping != null; ) {
            RDN rdn = (RDN)i.next();

            Collection sourceMappings = entryMapping.getSourceMappings();
            for (Iterator j=sourceMappings.iterator(); j.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)j.next();
                extractSourceValues(interpreter, rdn, entryMapping, sourceMapping, sourceValues);
            }

            entryMapping = partition.getParent(entryMapping);
        }
    }

    public void extractSourceValues(
            Interpreter interpreter,
            RDN rdn,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            AttributeValues sourceValues
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        interpreter.clear();
        interpreter.set(rdn);

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            String fieldName = sourceMapping.getName()+"."+fieldMapping.getName();
            sourceValues.set(fieldName, value);
            if (debug) log.debug(" => "+fieldName+": "+value);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            Entry parent,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

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

                connector.add(partition, sourceConfig, sv, request, response);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();
        String password = request.getPassword();

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
                    connector.bind(partition, sourceConfig, entryMapping, pk, request, response);
                    return;
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

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

                connector.delete(partition, sourceConfig, sv, request, response);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();
        Collection modifications = request.getModifications();

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
        if (debug) log.debug("Primary source: "+primarySourceMapping.getName());

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            if (debug) log.debug("Modifying source "+sourceMapping.getName());

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

                    int op = mi.getType();
                    Attribute attribute = mi.getAttribute();
                    if (!attribute.getName().equals(variable)) continue;

                    if (debug) log.debug("Converting modification for attribute "+variable);

                    Attribute newAttr = new Attribute(name);
                    newAttr.setValues(attribute.getValues());

                    mods.add(new Modification(op, newAttr));

                    if ((!sourceMapping.equals(primarySourceMapping)) && fieldConfig.isPrimaryKey()) {
                        deleteExistingEntries = true;
                        if (debug) log.debug("Removing field "+name);
                        rb.remove(name);
                    }
                }
            }

            RDN pk2 = rb.toRdn();

            if (debug) log.debug("PK: "+pk);
            if (debug) log.debug("PK2: "+pk2);

            if (deleteExistingEntries) {
                Connection connection = connector.getConnection(partition, sourceConfig.getConnectionName());
                connection.delete(sourceConfig, pk2, null, null);
            }

            for (Iterator j=entries.keySet().iterator(); j.hasNext(); ) {
                pk = (RDN)j.next();
                AttributeValues sv = (AttributeValues)entries.get(pk);
                if (debug) log.debug("Modifying entry "+pk+" in "+sourceConfig.getName()+": "+sv);

                connector.modify(
                        partition,
                        sourceConfig,
                        pk,
                        mods,
                        sv,
                        newAttributeValues,
                        request,
                        response
                );
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            Entry entry,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();
        RDN newRdn = request.getNewRdn();
        boolean deleteOldRdn = request.getDeleteOldRdn();

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

            connector.modrdn(
                    partition,
                    sourceConfig,
                    oldPk,
                    newPk,
                    deleteOldRdn,
                    request,
                    response
            );
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Partition partition,
            AttributeValues sourceValues,
            EntryMapping baseMapping,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        final DN baseDn = request.getDn();
        final Filter filter = request.getFilter();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Base DN       : "+baseDn, 80));
            log.debug(Formatter.displayLine("Base Mapping  : "+baseMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Filter        : "+filter, 80));
            log.debug(Formatter.displayLine("Scope         : "+LDAPUtil.getScope(request.getScope()), 80));
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

            SearchResponse sr = new BasicEngineSearchResponse(response, interpreterManager.newInstance());

            searchEngine.search(
                    partition,
                    sourceValues,
                    entryMapping,
                    request,
                    sr
            );

        } finally {
            response.close();
        }
    }
}
