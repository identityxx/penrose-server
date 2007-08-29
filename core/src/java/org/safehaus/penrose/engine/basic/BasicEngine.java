package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.ldap.*;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class BasicEngine extends Engine {

    public final static String  FETCH         = "fetch";
    public final static boolean DEFAULT_FETCH = false;

    boolean fetch = DEFAULT_FETCH;

    public void init() throws Exception {
        super.init();

        String s = getParameter(FETCH);
        if (s != null) fetch = Boolean.valueOf(s);

        log.debug("Default engine initialized.");
    }

    public void extractSourceValues(
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            SourceValues sourceValues
    ) throws Exception {

        Interpreter interpreter = interpreterManager.newInstance();

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
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        EntryMapping em = partitionConfig.getDirectoryConfigs().getParent(entryMapping);

        if (parentDn != null && em != null) {
            extractSourceValues(partition, interpreter, parentDn, em, sourceValues);
        }

        RDN rdn = dn.getRdn();
        Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();

        //if (sourceMappings.isEmpty()) return;
        //SourceMapping sourceMapping = sourceMappings.iterator().next();

        //interpreter.set(sourceValues);
        interpreter.set(rdn);

        for (SourceMapping sourceMapping : sourceMappings) {
            extractSourceValues(
                    partition,
                    interpreter,
                    rdn,
                    entryMapping,
                    sourceMapping,
                    sourceValues
            );
        }

        interpreter.clear();
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            RDN rdn,
            EntryMapping entryMapping,
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

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if ("INTEGER".equals(fieldConfig.getType()) && value instanceof String) {
                value = Integer.parseInt((String)value);
            }

            attributes.addValue(fieldMapping.getName(), value);

            String fieldName = sourceMapping.getName() + "." + fieldMapping.getName();
            if (debug) log.debug(" => " + fieldName + ": " + value);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues, AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryMapping.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.add(
                session,
                partition,
                entryMapping,
                localSourceRefs,
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
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);

        boolean success = true;
        boolean found = false;

        for (Collection<SourceRef> sourceRefs : groupsOfSources) {

            SourceRef sourceRef = sourceRefs.iterator().next();
            Connector connector = getConnector(sourceRef);

            String flag = sourceRef.getBind();
            if (debug) log.debug("Flag: "+flag);
            
            if (SourceMapping.IGNORE.equals(flag)) {
                continue;
            }

            found |= flag != null;

            try {
                connector.bind(
                        session,
                        partition,
                        entryMapping,
                        sourceRefs,
                        sourceValues,
                        request,
                        response
                );

                if (flag == null || SourceMapping.SUFFICIENT.equals(flag)) {
                    if (debug) log.debug("Bind is sufficient.");
                    return;
                }

            } catch (Exception e) {

                if (debug) log.debug(e.getMessage());

                if (SourceMapping.REQUISITE.equals(flag)) {
                    if (debug) log.debug("Bind is requisite.");
                    log.error(e.getMessage(), e);
                    throw e;
                    
                } else {
                    success = false;
                }
            }
        }

        if (!found || !success) {
            log.debug("Calling default bind operation.");
            super.bind(session, partition, entryMapping, sourceValues, request, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("COMPARE", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);

        for (Collection<SourceRef> sourceRefs : groupsOfSources) {

            SourceRef sourceRef = sourceRefs.iterator().next();
            Connector connector = getConnector(sourceRef);

            connector.compare(
                    session,
                    partition,
                    entryMapping,
                    sourceRefs,
                    sourceValues,
                    request,
                    response
            );

            return;
        }

        log.debug("Calling default compare operation.");
        super.compare(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryMapping.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.delete(
                session,
                partition,
                entryMapping,
                localSourceRefs,
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
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryMapping.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.modify(
                session,
                partition,
                entryMapping,
                localSourceRefs,
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
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entryMapping.getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.modrdn(
                session,
                partition,
                entryMapping,
                localSourceRefs,
                sourceValues,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entryMapping, dn, sourceValues);
        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse<SearchResult> response = new SearchResponse<SearchResult>();

        List<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entryMapping);
        Interpreter interpreter = getInterpreterManager().newInstance();

        if (groupsOfSources.isEmpty()) {
            if (debug) log.debug("Returning static entry "+entryMapping.getDn());

            interpreter.set(sourceValues);
            Attributes attributes = computeAttributes(interpreter, entryMapping);
            interpreter.clear();

            SearchResult searchResult = new SearchResult(entryMapping.getDn(), attributes);
            searchResult.setEntryMapping(entryMapping);
            searchResult.setSourceValues(sourceValues);

            return searchResult;
        }

        Collection<SourceRef> sourceRefs = groupsOfSources.get(0);
        Connector connector = getConnector(sourceRefs.iterator().next());

        BasicSearchResponse sr = new BasicSearchResponse(
                session,
                partition,
                this,
                entryMapping,
                groupsOfSources,
                sourceValues,
                interpreter,
                request,
                response
        );

        connector.search(
                session,
                partition,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                sr
        );

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        return response.next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Partition partition,
            EntryMapping baseMapping,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        DN dn = request.getDn();
        int scope = request.getScope();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, baseMapping, dn);
            sourceValues.add(entry.getSourceValues());

            if (entryMapping == baseMapping) {

                if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {
                    response.add(entry);
                    response.close();
                    return;
                }
            }
            
        } else {
           extractSourceValues(partition, baseMapping, dn, sourceValues);
        }

        //EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Base DN       : "+dn, 80));
            log.debug(Formatter.displayLine("Base Mapping  : "+baseMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entryMapping.getDn(), 80));
            log.debug(Formatter.displayLine("Filter        : "+request.getFilter(), 80));
            log.debug(Formatter.displayLine("Scope         : "+ LDAP.getScope(request.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        //List<Collection<SourceRef>> groupsOfSources = getLocalGroupsOfSources(partition, baseMapping, entryMapping);
        List<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, baseMapping, entryMapping);
        Interpreter interpreter = getInterpreterManager().newInstance();

        if (groupsOfSources.isEmpty()) {
            if (debug) log.debug("Returning static entry "+entryMapping.getDn());

            interpreter.set(sourceValues);
            Attributes attributes = computeAttributes(interpreter, entryMapping);
            interpreter.clear();

            SearchResult searchResult = new SearchResult(entryMapping.getDn(), attributes);
            searchResult.setEntryMapping(entryMapping);
            searchResult.setSourceValues(sourceValues);
            response.add(searchResult);

            response.close();

            return;
        }

        Collection<SourceRef> sourceRefs = groupsOfSources.get(0);
        Connector connector = getConnector(sourceRefs.iterator().next());

        BasicSearchResponse sr = new BasicSearchResponse(
                session,
                partition,
                this,
                entryMapping,
                groupsOfSources,
                sourceValues,
                interpreter,
                request,
                response
        );

        connector.search(
                session,
                partition,
                entryMapping,
                sourceRefs,
                sourceValues,
                request,
                sr
        );
    }

    public Attributes computeAttributes(
            Interpreter interpreter,
            EntryMapping entryMapping
    ) throws Exception {

        Attributes attributes = new Attributes();

        Collection<AttributeMapping> attributeMappings = entryMapping.getAttributeMappings();

        for (AttributeMapping attributeMapping : attributeMappings) {

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection) value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        Collection<String> objectClasses = entryMapping.getObjectClasses();
        for (String objectClass : objectClasses) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }
}
