package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.ldap.*;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BasicEngine extends Engine {

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
            SourceValues sourceValues
    ) throws Exception {

        Interpreter interpreter = interpreterManager.newInstance();

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Extracting source values from "+dn);

        extractSourceValues(
                partition,
                interpreter,
                dn,
                entryMapping,
                sourceValues
        );
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            DN dn,
            EntryMapping entryMapping,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        EntryMapping em = partition.getParent(entryMapping);

        if (parentDn != null && em != null) {
            extractSourceValues(partition, interpreter, parentDn, em, sourceValues);
        }

        RDN rdn = dn.getRdn();
        Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();

        for (SourceMapping sourceMapping : sourceMappings) {
            extractSourceValues(
                    interpreter,
                    rdn,
                    entryMapping,
                    sourceMapping,
                    sourceValues
            );
        }
    }

    public void extractSourceValues(
            Interpreter interpreter,
            RDN rdn,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceValues sourceValues
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        interpreter.set(sourceValues);
        interpreter.set(rdn);

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            String fieldName = sourceMapping.getName()+"."+fieldMapping.getName();
            sourceValues.set(fieldName, value);
            if (debug) log.debug(" => "+fieldName+": "+value);
        }

        interpreter.clear();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection groupsOfSources = createGroupsOfSources(partition, entryMapping);

        Iterator iterator = groupsOfSources.iterator();
        Collection primarySources = (Collection)iterator.next();

        SourceRef sourceRef = (SourceRef)primarySources.iterator().next();
        Connector connector = getConnector(sourceRef);

        connector.add(
                partition,
                entryMapping,
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
            Partition partition,
            EntryMapping entryMapping,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection groupsOfSources = createGroupsOfSources(partition, entryMapping);

        Iterator iterator = groupsOfSources.iterator();
        Collection primarySources = (Collection)iterator.next();

        SourceRef sourceRef = (SourceRef)primarySources.iterator().next();
        Connector connector = getConnector(sourceRef);

        try {
            connector.bind(
                    partition,
                    entryMapping,
                    primarySources,
                    sourceValues,
                    request,
                    response
            );

        } catch (LDAPException e) {
            if (e.getResultCode() == LDAPException.INVALID_CREDENTIALS) {
                log.debug("Calling default bind operation.");
                super.bind(session, partition, entryMapping, request, response);
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
            Partition partition,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection groupsOfSources = createGroupsOfSources(partition, entryMapping);

        Iterator iterator = groupsOfSources.iterator();
        Collection primarySources = (Collection)iterator.next();

        SourceRef sourceRef = (SourceRef)primarySources.iterator().next();
        Connector connector = getConnector(sourceRef);

        connector.delete(
                partition,
                entryMapping,
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
            Partition partition,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection groupsOfSources = createGroupsOfSources(partition, entryMapping);

        Iterator iterator = groupsOfSources.iterator();
        Collection primarySources = (Collection)iterator.next();

        SourceRef sourceRef = (SourceRef)primarySources.iterator().next();
        Connector connector = getConnector(sourceRef);

        connector.modify(
                partition,
                entryMapping,
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
            Partition partition,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Collection groupsOfSources = createGroupsOfSources(partition, entryMapping);

        Iterator iterator = groupsOfSources.iterator();
        Collection primarySources = (Collection)iterator.next();

        SourceRef sourceRef = (SourceRef)primarySources.iterator().next();
        Connector connector = getConnector(sourceRef);

        connector.modrdn(
                partition,
                entryMapping,
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
            Partition partition,
            EntryMapping baseMapping,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Base DN       : "+request.getDn(), 80));
            log.debug(Formatter.displayLine("Base Mapping  : "+baseMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Filter        : "+request.getFilter(), 80));
            log.debug(Formatter.displayLine("Scope         : "+LDAPUtil.getScope(request.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        try {
            extractSourceValues(partition, baseMapping, request.getDn(), sourceValues);
            EngineTool.propagateDown(partition, entryMapping, sourceValues);

            if (debug) {
                log.debug("Source values:");
                sourceValues.print();
            }

            searchEngine.search(
                    partition,
                    baseMapping,
                    entryMapping,
                    sourceValues,
                    request,
                    response
            );

        } finally {
            response.close();
        }
    }
}
