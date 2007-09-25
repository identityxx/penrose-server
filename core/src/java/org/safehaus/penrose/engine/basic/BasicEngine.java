package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.FieldMapping;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.filter.Filter;

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
            Entry entry,
            DN dn,
            SourceValues sourceValues
    ) throws Exception {

        Interpreter interpreter = interpreterManager.newInstance();

        if (debug) log.debug("Extracting source values from "+dn);

        extractSourceValues(
                partition,
                interpreter,
                dn,
                entry,
                sourceValues
        );
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            DN dn,
            Entry entry,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        Entry parent = entry.getParent();

        if (parentDn != null && parent != null) {
            extractSourceValues(partition, interpreter, parentDn, parent, sourceValues);
        }

        RDN rdn = dn.getRdn();
        Collection<SourceMapping> sourceMappings = entry.getSourceMappings();

        //if (sourceMappings.isEmpty()) return;
        //SourceMapping sourceMapping = sourceMappings.iterator().next();

        //interpreter.set(sourceValues);
        interpreter.set(rdn);

        for (SourceMapping sourceMapping : sourceMappings) {
            extractSourceValues(
                    partition,
                    interpreter,
                    rdn,
                    entry,
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
            Entry entry,
            SourceMapping sourceMapping,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        Attributes attributes = sourceValues.get(sourceMapping.getName());

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SourceConfigs sourceConfigs = partitionConfig.getSourceConfigs();
        SourceConfig sourceConfig = sourceConfigs.getSourceConfig(sourceMapping.getSourceName());

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

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entry.getEntryMapping().getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.add(
                session,
                entry,
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

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

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
                        entry,
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
            super.bind(session, entry, sourceValues, request, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Entry entry,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("COMPARE", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        for (Collection<SourceRef> sourceRefs : groupsOfSources) {

            SourceRef sourceRef = sourceRefs.iterator().next();
            Connector connector = getConnector(sourceRef);

            connector.compare(
                    session,
                    entry,
                    sourceRefs,
                    sourceValues,
                    request,
                    response
            );

            return;
        }

        log.debug("Calling default compare operation.");
        super.compare(session, entry, sourceValues, request, response);
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
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entry.getEntryMapping().getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.delete(
                session,
                entry,
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
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entry.getEntryMapping().getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.modify(
                session,
                entry,
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
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        Collection<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);

        Iterator<Collection<SourceRef>> iterator = groupsOfSources.iterator();

        Collection<SourceRef> sourceRefs = iterator.next();
        Connector connector = getConnector(sourceRefs.iterator().next());

        Collection<SourceRef> localSourceRefs = new ArrayList<SourceRef>();

        for (SourceRef sourceRef : sourceRefs) {
            if (entry.getEntryMapping().getSourceMapping(sourceRef.getAlias()) != null) {
                localSourceRefs.add(sourceRef);
            }
        }

        connector.modrdn(
                session,
                entry,
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
            Entry entry,
            DN dn
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("FIND", 80));
            log.debug(Formatter.displayLine("DN            : "+dn, 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+entry.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceValues sourceValues = new SourceValues();
        extractSourceValues(partition, entry, dn, sourceValues);
        EngineTool.propagateDown(entry, sourceValues);

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        List<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, entry);
        Interpreter interpreter = getInterpreterManager().newInstance();

        if (groupsOfSources.isEmpty()) {
            if (debug) log.debug("Returning static entry "+entry.getDn());

            interpreter.set(sourceValues);
            Attributes attributes = computeAttributes(interpreter, entry);
            interpreter.clear();

            SearchResult searchResult = new SearchResult(entry.getDn(), attributes);
            searchResult.setEntry(entry);
            searchResult.setSourceValues(sourceValues);

            return searchResult;
        }

        Collection<SourceRef> sourceRefs = groupsOfSources.get(0);
        Connector connector = getConnector(sourceRefs.iterator().next());

        BasicSearchResponse sr = new BasicSearchResponse(
                session,
                partition,
                this,
                entry,
                groupsOfSources,
                sourceValues,
                interpreter,
                request,
                response
        );

        Collection<SourceRef> primarySourceRefs = entry.getPrimarySourceRefs();
        Collection<SourceRef> localSourceRefs = entry.getLocalSourceRefs();

        connector.search(
                session,
                primarySourceRefs,
                localSourceRefs,
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
            final Session session,
            final Entry base,
            final Entry entry,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Base DN       : "+request.getDn(), 80));
            log.debug(Formatter.displayLine("Base Mapping  : "+ base.getDn(), 80));
            log.debug(Formatter.displayLine("Entry Mapping : "+ entry.getDn(), 80));
            log.debug(Formatter.displayLine("Filter        : "+request.getFilter(), 80));
            log.debug(Formatter.displayLine("Scope         : "+ LDAP.getScope(request.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();
        final Filter filter = request.getFilter();

        //List<Collection<SourceRef>> groupsOfSources = getLocalGroupsOfSources(partition, base, entry);
        List<Collection<SourceRef>> groupsOfSources = getGroupsOfSources(partition, base, entry);
        Interpreter interpreter = getInterpreterManager().newInstance();

        if (groupsOfSources.isEmpty()) {
            if (debug) log.debug("Returning static entry "+ entry.getDn());

            interpreter.set(sourceValues);
            Attributes attributes = computeAttributes(interpreter, entry);
            interpreter.clear();

            if (filterEvaluator.eval(attributes, filter)) { // Check LDAP filter
                SearchResult searchResult = new SearchResult(entry.getDn(), attributes);
                searchResult.setEntry(entry);
                searchResult.setSourceValues(sourceValues);
                response.add(searchResult);
            }

            return;
        }

        if (!filterEvaluator.eval(entry, filter)) { // Check LDAP filter
            if (debug) log.debug("Entry \""+entry.getDn()+"\" doesn't match search filter.");
            return;
        }

        Collection<SourceRef> sourceRefs = groupsOfSources.get(0);
        Connector connector = getConnector(sourceRefs.iterator().next());

        SearchResponse sr = new SearchResponse() {

            public void add(SearchResult searchResult) throws Exception {

                if (debug) log.debug("Checking filter "+filter);

                if (!filterEvaluator.eval(searchResult.getAttributes(), filter)) { // Check LDAP filter
                    if (debug) log.debug("Entry \""+searchResult.getDn()+"\" doesn't match search filter.");
                    return;
                }

                response.add(searchResult);
            }
        };

        BasicSearchResponse sr2 = new BasicSearchResponse(
                session,
                partition,
                this,
                entry,
                groupsOfSources,
                sourceValues,
                interpreter,
                request,
                sr
        );

        Collection<SourceRef> primarySourceRefs = entry.getPrimarySourceRefs();
        Collection<SourceRef> localSourceRefs = entry.getLocalSourceRefs();

        connector.search(
                session,
                primarySourceRefs,
                localSourceRefs,
                sourceRefs,
                sourceValues,
                request,
                sr2
        );
    }

    public Attributes computeAttributes(
            Interpreter interpreter,
            Entry entry
    ) throws Exception {

        Attributes attributes = new Attributes();

        Collection<AttributeMapping> attributeMappings = entry.getAttributeMappings();

        for (AttributeMapping attributeMapping : attributeMappings) {

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            if (value instanceof Collection) {
                attributes.addValues(attributeMapping.getName(), (Collection) value);
            } else {
                attributes.addValue(attributeMapping.getName(), value);
            }
        }

        Collection<String> objectClasses = entry.getObjectClasses();
        for (String objectClass : objectClasses) {
            attributes.addValue("objectClass", objectClass);
        }

        return attributes;
    }
}
