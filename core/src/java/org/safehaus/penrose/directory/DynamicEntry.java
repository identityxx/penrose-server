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
package org.safehaus.penrose.directory;

import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.util.TransformationUtil;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.pipeline.SOPipeline;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingRule;
import org.safehaus.penrose.password.Password;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DynamicEntry extends Entry implements Cloneable {

    public final static String  FETCH         = "fetch";
    public final static boolean DEFAULT_FETCH = false; // disabled

    protected boolean fetch;

    public void init() throws Exception {
        String s = getParameter(FETCH);
        fetch = s == null ? DEFAULT_FETCH : Boolean.valueOf(s);

        super.init();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("ADD", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        validatePermission(session, request);
        validateSchema(request);

        Attributes attributes = request.getAttributes();

        SourceAttributes sourceValues;

        if (fetch) {
            Entry parent = getParent();
            DN parentDn = parent.getDn();

            SearchResult sr = parent.find(session, parentDn);

            sourceValues = new SourceAttributes();
            sourceValues.add(sr.getSourceAttributes());

        } else {
            sourceValues = extractSourceValues(request, dn, attributes);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        for (Attribute attribute : attributes.getAll()) {
            String attributeName = attribute.getName();
            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(attributeName, attributeValue);
        }

        for (EntrySource source : getSources()) {
            add(session, request, response, sourceValues, interpreter, source);
        }
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response,
            SourceAttributes sourceValues,
            Interpreter interpreter,
            EntrySource source
    ) throws Exception {

        if (debug) log.debug("Adding source "+source.getAlias()+".");

        Attributes sourceAttributes = sourceValues.get(source.getAlias());

        if (debug) log.debug("Target attributes:");

        Attributes newAttributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();
        for (EntryField field : source.getFields()) {

            Collection<String> operations = field.getOperations();
            if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.ADD)) continue;

            Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) value = interpreter.eval(field);
            if (value == null) continue;

            String encryption = field.getEncryption();
            if (encryption != null) value = encrypt(encryption, value);

            String fieldName = field.getOriginalName();
            boolean primaryKey = field.getField().isPrimaryKey();

            if (debug) log.debug(" - " + fieldName + ": " + value + (primaryKey ? " (pk)" : ""));

            newAttributes.addValue(fieldName, value);
            if (primaryKey) rb.set(fieldName, value);
        }

        Object targetDn = sourceAttributes == null ? null : sourceAttributes.getValue("dn");

        if (targetDn == null) targetDn = rb.toRdn().toString();
        log.debug("Target DN: "+targetDn);

        AddRequest newRequest = new AddRequest(request);
        newRequest.setDn(targetDn.toString());
        newRequest.setAttributes(newAttributes);

        source.add(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();
        byte[] password = request.getPassword();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC BIND", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        SourceAttributes sourceValues = new SourceAttributes();

        if (fetch) {
            SearchResult sr = find(dn);
            sourceValues.add(sr.getSourceAttributes());
        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        boolean success = true;
        boolean found = false;

        for (EntrySource source : getSources()) {

            if (debug) log.debug("Binding to source "+source.getAlias());

            Attributes attributes = sourceValues.get(source.getAlias());
            Object sourceDn = attributes == null ? null : attributes.getValue("dn");
            if (debug) log.debug("Source DN: "+sourceDn);

            String flag = source.getBind();
            if (debug) log.debug("Flag: "+flag);

            if (EntrySourceConfig.IGNORE.equals(flag)) {
                continue;
            }

            found |= flag != null;

            try {
                BindRequest sourceRequest = new BindRequest();
                sourceRequest.setDn(sourceDn == null ? null : sourceDn.toString());
                sourceRequest.setPassword(password);

                BindResponse sourceResponse = new BindResponse();

                source.bind(session, sourceRequest, sourceResponse, attributes);

                if (flag == null || EntrySourceConfig.SUFFICIENT.equals(flag)) {
                    if (debug) log.debug("Bind is sufficient.");
                    return;
                }

            } catch (Exception e) {

                log.error(e.getMessage());

                if (EntrySourceConfig.REQUISITE.equals(flag)) {
                    if (debug) log.debug("Bind is requisite.");
                    throw e;

                } else {
                    success = false;
                }
            }
        }

        if (!found || !success) {
            log.debug("Calling default bind operation.");
            super.bind(session, request, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC COMPARE", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        validatePermission(session, request);

        SourceAttributes sourceValues = new SourceAttributes();

        if (fetch) {
            SearchResult sr = find(dn);
            sourceValues.add(sr.getSourceAttributes());

        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        EntrySource source = getSource();
        Attributes sourceAttributes = sourceValues.get(source.getAlias());

        Object targetDn = sourceAttributes == null ? null : sourceAttributes.getValue("dn");

        if (targetDn == null) {

            if (debug) log.debug("Computing target DN:");

            RDNBuilder rb = new RDNBuilder();
            for (EntryField field : source.getPrimaryKeyFields()) {

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.DELETE)) continue;

                Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
                Object value = attribute == null ? null : attribute.getValue();

                if (value == null) value = interpreter.eval(field);
                if (value == null) continue;

                String fieldName = field.getOriginalName();
                if (debug) log.debug(" - "+fieldName+": "+value);

                rb.set(fieldName, value);
            }

            targetDn = rb.toRdn().toString();
        }

        if (debug) log.debug("Target DN: "+targetDn);

        CompareRequest newRequest = (CompareRequest)request.clone();
        newRequest.setDn(targetDn.toString());

        interpreter.clear();
        interpreter.set(request.getAttributeName(), request.getAttributeValue());

        for (EntryField field : source.getFields()) {

            Collection<String> operations = field.getOperations();
            if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.COMPARE)) continue;

            Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) value = interpreter.eval(field);
            if (value == null) continue;

            if (value instanceof Collection) {
                Collection list = (Collection)value;
                value = list.iterator().next();
            }

            String fieldName = field.getOriginalName();

            if (debug) {
                String v;
                if (value instanceof byte[]) {
                    v = new String((byte[])value);
                } else {
                    v = value.toString();
                }

                log.debug("Comparing field " + fieldName + " = [" + v + "]");
            }

            newRequest.setAttributeName(fieldName);
            newRequest.setAttributeValue(value);

            break;
        }

        source.compare(session, newRequest, response);
        
        //log.debug("Calling default compare operation.");
        //super.compare(session, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC DELETE", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        validatePermission(session, request);

        SourceAttributes sourceValues = new SourceAttributes();

        if (fetch) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceAttributes());
        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        for (EntrySource source : getSources()) {
            delete(session, request, response, sourceValues, interpreter, source);
        }
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response,
            SourceAttributes sourceValues,
            Interpreter interpreter,
            EntrySource source
    ) throws Exception {

        if (debug) log.debug("Deleting source "+source.getAlias()+".");

        Attributes sourceAttributes = sourceValues.get(source.getAlias());

        Object targetDn = sourceAttributes == null ? null : sourceAttributes.getValue("dn");

        if (targetDn == null) {

            if (debug) log.debug("Computing target DN:");

            RDNBuilder rb = new RDNBuilder();

            for (EntryField field : source.getPrimaryKeyFields()) {

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.DELETE)) continue;

                Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
                Object value = attribute == null ? null : attribute.getValue();

                if (value == null) value = interpreter.eval(field);
                if (value == null) continue;

                String fieldName = field.getOriginalName();
                if (debug) log.debug(" - "+fieldName+": "+value);

                rb.set(fieldName, value);
            }

            targetDn = rb.toRdn().toString();
        }

        if (debug) log.debug("Target DN: "+targetDn);

        DeleteRequest newRequest = new DeleteRequest(request);
        newRequest.setDn(targetDn.toString());

        source.delete(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC MODIFY", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        validatePermission(session, request);
        validateSchema(request);

        SourceAttributes sourceValues = new SourceAttributes();

        if (fetch) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceAttributes());
        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        for (EntrySource source : getSources()) {
            modify(session, request, response, sourceValues, interpreter, source);
        }
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response,
            SourceAttributes sourceValues,
            Interpreter interpreter,
            EntrySource source
    ) throws Exception {

        if (debug) log.debug("Modifying source "+source.getAlias()+".");

        Attributes sourceAttributes = sourceValues.get(source.getAlias());

        Object targetDn = sourceAttributes == null ? null : sourceAttributes.getValue("dn");

        if (targetDn == null) {

            if (debug) log.debug("Computing target DN:");
            RDNBuilder rb = new RDNBuilder();

            for (EntryField field : source.getPrimaryKeyFields()) {

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.MODIFY)) continue;

                Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
                Object value = attribute == null ? null : attribute.getValue();

                if (value == null) value = interpreter.eval(field);
                if (value == null) continue;

                String fieldName = field.getOriginalName();
                if (debug) log.debug(" - "+fieldName+": "+value);

                rb.set(fieldName, value);
            }

            targetDn = rb.toRdn().toString();
        }
        
        if (debug) log.debug("Target DN: "+targetDn);

        Collection<Modification> newModifications = new ArrayList<Modification>();

        Collection<Modification> modifications = request.getModifications();
        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute modificationAttribute = modification.getAttribute();

            String attributeName = modificationAttribute.getName();
            Collection attributeValues = modificationAttribute.getValues();

            if (debug) {
                switch (type) {
                    case Modification.ADD:
                        log.debug("Adding attribute " + attributeName + ": " + attributeValues);
                        break;
                    case Modification.REPLACE:
                        log.debug("Replacing attribute " + attributeName + ": " + attributeValues);
                        break;
                    case Modification.DELETE:
                        log.debug("Deleting attribute " + attributeName + ": " + attributeValues);
                        break;
                }
            }

            interpreter.clear();
            interpreter.set(sourceValues);
            interpreter.set(attributeName, attributeValues);

            switch (type) {
                case Modification.ADD:
                case Modification.REPLACE:
                    for (EntryField field : source.getFields()) {

                        Collection<String> operations = field.getOperations();
                        if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.MODIFY)) continue;

                        String fieldName = field.getName();
                        if (field.isPrimaryKey()) continue;

                        Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
                        Object value = attribute == null ? null : attribute.getValue();

                        if (value == null) value = interpreter.eval(field);
                        if (value == null) continue;

                        String encryption = field.getEncryption();
                        if (encryption != null) value = encrypt(encryption, value);

                        if (debug) log.debug(" => Replacing field " + fieldName + ": " + value);

                        Attribute newAttribute = new Attribute(field.getOriginalName());
                        if (value instanceof Collection) {
                            Collection list = (Collection)value;
                            for (Object v : list) {
                                newAttribute.addValue(v);
                            }
                        } else {
                            newAttribute.addValue(value);
                        }
                        newModifications.add(new Modification(type, newAttribute));
                    }
                    break;

                case Modification.DELETE:
                    for (EntryField field : source.getFields()) {

                        Collection<String> operations = field.getOperations();
                        if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.MODIFY)) continue;

                        String fieldName = field.getName();
                        if (field.isPrimaryKey()) continue;

                        Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
                        Object value = attribute == null ? null : attribute.getValue();

                        if (value == null) value = interpreter.eval(field);
                        if (value == null) continue;

                        String encryption = field.getEncryption();
                        if (encryption != null) value = encrypt(encryption, value);

                        if (debug) log.debug(" ==> Deleting field " + fieldName + ": "+value);

                        Attribute newAttribute = new Attribute(field.getOriginalName());
                        if (value instanceof Collection) {
                            Collection list = (Collection)value;
                            for (Object v : list) {
                                newAttribute.addValue(v);
                            }
                        } else {
                            newAttribute.addValue(value);
                        }
                        newModifications.add(new Modification(type, newAttribute));
/*
                        String variable = field.getVariable();
                        if (variable == null) {

                        } else {
                            if (!variable.equals(attributeName)) continue;

                            if (debug) log.debug(" ==> Deleting field " + fieldName + ": "+attributeValues);

                            Attribute newAttribute = new Attribute(field.getOriginalName());
                            for (Object value : attributeValues) {
                                newAttribute.addValue(value);
                            }
                            newModifications.add(new Modification(type, newAttribute));
                        }
*/
                    }
                    break;
            }
        }

        ModifyRequest newRequest = new ModifyRequest();
        newRequest.setDn(targetDn.toString());
        newRequest.setModifications(newModifications);

        source.modify(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC MODRDN", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        validatePermission(session, request);
        validateSchema(request);

        SourceAttributes sourceValues = new SourceAttributes();

        if (fetch) {
            SearchResult sr = find(session, dn);
            sourceValues.add(sr.getSourceAttributes());

        } else {
            extractSourceValues(dn, sourceValues);
        }

        propagate(sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        for (EntrySource source : getSources()) {
            modrdn(session, request, response, sourceValues, interpreter, source);
        }
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response,
            SourceAttributes sourceValues,
            Interpreter interpreter,
            EntrySource source
    ) throws Exception {

        if (debug) log.debug("Renaming source "+source.getAlias()+".");

        Attributes sourceAttributes = sourceValues.get(source.getAlias());

        Object targetDn = sourceAttributes == null ? null : sourceAttributes.getValue("dn");

        if (targetDn == null) {

            if (debug) log.debug("Computing target DN:");

            RDNBuilder rb = new RDNBuilder();
            for (EntryField field : source.getPrimaryKeyFields()) {

                Collection<String> operations = field.getOperations();
                if (!operations.isEmpty() && !operations.contains(EntryFieldConfig.MODRDN)) continue;

                Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
                Object value = attribute == null ? null : attribute.getValue();

                if (value == null) value = interpreter.eval(field);
                if (value == null) continue;

                String fieldName = field.getOriginalName();
                if (debug) log.debug(" - "+fieldName+": "+value);

                rb.set(fieldName, value);
            }

            targetDn = rb.toRdn().toString();
        }

        interpreter.clear();
        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (String attributeName : newRdn.getNames()) {
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        RDNBuilder rb = new RDNBuilder();
        for (EntryField field : source.getPrimaryKeyFields()) {
 
            Attribute attribute = sourceAttributes == null ? null : sourceAttributes.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) value = interpreter.eval(field);
            if (value == null) continue;

            rb.set(field.getOriginalName(), value);
        }

        ModRdnRequest newRequest = new ModRdnRequest(request);
        newRequest.setDn(targetDn.toString());
        newRequest.setNewRdn(rb.toRdn());

        source.modrdn(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchOperation operation
    ) throws Exception {

        final DN baseDn     = operation.getDn();
        final Filter filter = operation.getFilter();
        final int scope     = operation.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC SEARCH", 70));
            log.debug(TextUtil.displayLine("Class  : "+getClass().getName(), 70));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 70));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 70));
            log.debug(TextUtil.displayLine("Filter : "+filter, 70));
            log.debug(TextUtil.displayLine("Scope  : "+LDAP.getScope(scope), 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        EntrySearchOperation op = new EntrySearchOperation(operation, this);

        try {
            if (!validate(op)) return;

            expand(op);

        } finally {
            op.close();
        }
    }
/*
    public void expand(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) log.debug("Expanding dynamic entry.");

        DN baseDn = request.getDn();
        
        SourceAttributes sourceValues = new SourceAttributes();

        if (fetch) {
            SearchResult result = find(session, baseDn);
            sourceValues.add(result.getSourceAttributes());

            response.add(result);
            return;

        } else {
            extractSourceValues(baseDn, sourceValues);
        }

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Interpreter interpreter = partition.newInterpreter();

        List<Collection<EntrySource>> groupsOfSources = getGroupsOfSources(
                request
        );

        Collection<EntrySource> sourceRefs = groupsOfSources.get(0);

        SearchRequest sourceRequest = (SearchRequest)request.clone();
        if (!getDn().matches(baseDn)) sourceRequest.setDn((DN)null);

        DynamicSearchResponse newResponse = new DynamicSearchResponse(
                this,
                session,
                groupsOfSources,
                sourceValues,
                interpreter,
                sourceRequest,
                response
        );

        Collection<EntrySource> localSourceRefs = getLocalSources();

        EntrySource sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        source.search(
                session,
                //primarySourceRefs,
                localSourceRefs,
                sourceRefs,
                sourceValues,
                sourceRequest,
                newResponse
        );

    }
*/

    public void expand(
            SearchOperation operation
    ) throws Exception {

        if (debug) log.debug("Expanding entry.");

        Session session = operation.getSession();

        DN baseDn = operation.getDn();
        int scope = operation.getScope();

        boolean baseSearch = (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB)
                && getDn().matches(baseDn);

        Filter filter = operation.getFilter();

        Collection<String> requestedAliases = getRequestedAliases(operation);
        if (debug) log.debug("Requested sources: "+requestedAliases);

        SourceAttributes sourceAttributes = new SourceAttributes();
        Interpreter interpreter = partition.newInterpreter();

        log.debug("Extracting source attributes from DN:");
        extractSourceAttributes(operation.getRequest(), baseDn, interpreter, sourceAttributes);

        if (debug) {
            log.debug("Source attributes:");
            sourceAttributes.print();
        }

        if (debug) log.debug("Search orders: "+searchOrders);

        Map<String,Filter> sourceFilters = createSourceFilters(filter, sourceAttributes, interpreter);
        Map<String,Boolean> requestedSources = createRequestedSources(operation, requestedAliases, sourceFilters);

        String primaryAlias  = getSearchOrder(0);
        Filter primaryFilter = sourceFilters.get(primaryAlias);

        if (!baseSearch && !sourceFilters.isEmpty()) {

            Map<DN,SourceAttributes> entries = new LinkedHashMap<DN,SourceAttributes>();

            for (String alias : searchOrders) {

                if (operation.isAbandoned()) {
                    if (debug) log.debug("Operation "+operation.getOperationName()+" has been abandoned.");
                    return;
                }

                Filter sourceFilter = sourceFilters.get(alias);
                if (sourceFilter == null) continue;

                if (debug) {
                    log.debug(TextUtil.displaySeparator(70));
                    log.debug(TextUtil.displayLine("Search source "+alias+" with filter "+sourceFilter+".", 70));
                    log.debug(TextUtil.displaySeparator(70));
                }

                SourceAttributes sa = new SourceAttributes();

                SearchRequest searchRequest = new SearchRequest();
                searchRequest.setFilter(sourceFilter);

                searchSource(session, sa, alias, searchRequest, entries);
            }

            log.debug("################################################################");

            for (DN dn : entries.keySet()) {

                if (operation.isAbandoned()) {
                    if (debug) log.debug("Operation "+operation.getOperationName()+" has been abandoned.");
                    return;
                }

                SourceAttributes sa = entries.get(dn);
                if (debug) log.debug("Returning "+dn+" with "+sa.getNames()+".");

                RDN rdn = dn.getRdn();
                Filter sourceFilter = null;
                for (String name : rdn.getNames()) {
                    Object value = rdn.get(name);
                    SimpleFilter sf = new SimpleFilter(name, "=", value);
                    sourceFilter = FilterTool.appendAndFilter(sourceFilter, sf);
                }

                SearchRequest newRequest = new SearchRequest();
                newRequest.setFilter(sourceFilter);

                SearchResponse newResponse = new SOPipeline(operation) {
                    public void close() throws Exception {
                    }
                };

                expandSource(session, newRequest, newResponse, sa, requestedSources);
            }

            log.debug("################################################################");
            return;
        }

        SourceAttributes sa = new SourceAttributes();

        SearchRequest newRequest = new SearchRequest();
        newRequest.setFilter(primaryFilter);

        SearchResponse newResponse = new SOPipeline(operation) {
            public void close() throws Exception {
            }
        };

        expandSource(session, newRequest, newResponse, sa, requestedSources);
    }

    public Collection<String> getRequestedAliases(SearchOperation operation) throws Exception {

        final Collection<String> requestedSources = new HashSet<String>();

        final Mapping mapping = getMapping();
        if (mapping != null) {

            FilterProcessor fp = new FilterProcessor() {
                public Filter process(Stack<Filter> path, Filter filter) throws Exception {
                    if (filter instanceof ItemFilter) {
                        ItemFilter f = (ItemFilter)filter;

                        String attributeName = f.getAttribute();

                        for (MappingRule rule : mapping.getRules(attributeName)) {
                            String variable = rule.getVariable();
                            if (variable == null) continue;

                            int i = variable.indexOf('.');
                            if (i < 0) continue;

                            String sourceName = variable.substring(0, i);
                            if (debug) log.debug("Filter attribute "+attributeName+": "+sourceName);
                            requestedSources.add(sourceName);
                        }

                    } else {
                        return super.process(path, filter);
                    }

                    return filter;
                }
            };

            fp.process(operation.getFilter());

            for (String attributeName : operation.getAttributes()) {
                for (MappingRule rule : mapping.getRules(attributeName)) {
                    String variable = rule.getVariable();
                    if (variable == null) continue;

                    int i = variable.indexOf('.');
                    if (i < 0) continue;

                    String sourceName = variable.substring(0, i);
                    if (debug) log.debug("Requested attribute "+attributeName+": "+sourceName);
                    requestedSources.add(sourceName);
                }
            }
        }

        return requestedSources;
    }

    public Map<String,Filter> createSourceFilters(Filter filter, SourceAttributes sourceAttributes, Interpreter interpreter) throws Exception {

        Map<String,Filter> filterMap = new HashMap<String,Filter>();

        FilterBuilder filterBuilder = new FilterBuilder(this, sourceAttributes, interpreter);

        for (EntrySource source : getSources()) {
            String alias = source.getAlias();
            Attributes attributes = sourceAttributes.get(alias);

            Filter sourceFilter = filterBuilder.convert(filter, source);
            sourceFilter = FilterTool.appendAndFilter(sourceFilter, createFilter(attributes));

            log.debug("Filter for "+alias+": "+sourceFilter);
            if (sourceFilter != null) filterMap.put(alias, sourceFilter);
        }

        return filterMap;
    }

    public Filter createFilter(Attributes attributes) throws Exception {

        Filter filter = null;

        for (Attribute attribute : attributes.getAll()) {
            String name = attribute.getName();
            for (Object value : attribute.getValues()) {
                SimpleFilter sf = new SimpleFilter(name, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
        }

        return filter;
    }

    public Map<String,Boolean> createRequestedSources(SearchOperation operation, Collection<String> requestedSources, Map<String,Filter> filterMap) throws Exception {

        Map<String,Boolean> requestedMap = new HashMap<String,Boolean>();

        Collection<String> attributeNames = operation.getAttributes();
        boolean allRequested = attributeNames.contains("*");

        for (EntrySource source : getSources()) {
            String alias = source.getAlias();

            Filter sourceFilter = filterMap.get(alias);
            boolean requested = allRequested || requestedSources.contains(alias) || sourceFilter != null;
            requestedMap.put(alias, requested);
        }

        return requestedMap;
    }

    public void searchSource(
            Session session,
            SourceAttributes sourceAttributes,
            String alias,
            SearchRequest searchRequest,
            Map<DN,SourceAttributes> results
    ) throws Exception {

        EntrySource source = getSource(alias);

        String linkedAttribute = null;
        String prevAlias = null;
        String prevLinkingAttribute = null;

        boolean firstSource;

        for (EntryField field : source.getFields()) {

            String variable = field.getVariable();
            if (variable == null) continue;
            
            int i = variable.indexOf(".");

            if (i < 0) continue;

            String s = variable.substring(0, i);
            if ("rdn".equals(s)) continue;

            linkedAttribute = field.getName();
            prevAlias = s;
            prevLinkingAttribute = variable.substring(i+1);

            break; // TODO need to support multiple link attributes
        }

        firstSource = prevAlias == null;

        try {
            SearchResponse searchResponse = new SearchResponse();

            source.search(session, searchRequest, searchResponse);

            while (searchResponse.hasNext()) {

                SearchResult result = searchResponse.next();

                SourceAttributes sa = (SourceAttributes)sourceAttributes.clone();
                sa.set(alias, result);

                if (debug) {
                    log.debug("Source attributes:");
                    sa.print();
                }

                if (firstSource) {
                    DN dn = createDn(sa);
                    if (debug) log.debug("Found "+dn+".");

                    results.put(dn, sa);

                    continue;
                }

                Collection<Object> prevLinks = sa.getValues(alias, linkedAttribute);

                for (Object prevLink : prevLinks) {

                    if (debug) log.debug("Following link "+prevLink+".");

                    SearchRequest prevSearchRequest = new SearchRequest();

                    if ("dn".equals(prevLinkingAttribute)) {
                        prevSearchRequest.setDn((String)prevLink);
                        prevSearchRequest.setScope(SearchRequest.SCOPE_BASE);

                    } else {
                        prevSearchRequest.setFilter(new SimpleFilter(prevLinkingAttribute, "=", prevLink));
                    }

                    searchSource(session, sa, prevAlias, prevSearchRequest, results);
                }
            }

        } catch (Exception e) {
            // ignore
        }
    }

    public void expandSource(
            Session session,
            SearchRequest request,
            SearchResponse response,
            SourceAttributes sourceAttributes,
            Map<String,Boolean> requestedSources
    ) throws Exception {
        expandSource(session, request, response, 0, sourceAttributes, requestedSources);
    }

    public void expandSource(
            final Session session,
            final SearchRequest request,
            final SearchResponse response,
            final int index,
            final SourceAttributes sourceAttributes,
            final Map<String,Boolean> requestedSources
    ) throws Exception {

        final String alias = getSearchOrder(index);

        int nextIndex = index+1;
        boolean lastSource = nextIndex == getSources().size();

        String nextAlias;

        String nla = null;
        String pa = null;
        String pla = null;

        if (!lastSource) {
            nextAlias = getSearchOrder(nextIndex);
            EntrySource nextSource = getSource(nextAlias);

            for (EntryField field : nextSource.getFields()) {

                String variable = field.getVariable();
                int i = variable.indexOf(".");

                if (i < 0) continue;

                nla = field.getName();
                pa = variable.substring(0, i);
                pla = variable.substring(i+1);

                break; // TODO need to support multiple link attributes
            }
        }

        final String nextLinkedAttribute = nla;
        final String prevAlias = pa;
        final String prevLinkingAttribute = pla;

        if (sourceAttributes.contains(alias)) {

            if (debug) log.debug("Source "+alias+" has been fetched.");

            expandSearchResult(
                    session, request, response, index, sourceAttributes, requestedSources,
                    nextLinkedAttribute, prevAlias, prevLinkingAttribute
            );

        } else {
            EntrySource source = getSource(alias);
            String search = source.getSearch();

            SearchResponse searchResponse = new SearchResponse() {
                public void add(SearchResult searchResult) throws Exception {

                    SourceAttributes sa  = (SourceAttributes)sourceAttributes.clone();
                    sa.set(alias, searchResult);

                    expandSearchResult(
                            session, request, response, index, sa, requestedSources,
                            nextLinkedAttribute, prevAlias, prevLinkingAttribute
                    );
                }
            };

            try {
                source.search(session, request, searchResponse);

            } catch (Exception e) {
                if (index == 0 || EntrySourceConfig.REQUIRED.equals(search)) {
                    if (debug) log.debug("Source "+alias+" is required and error occured.");

                } else {
                    if (debug) log.debug("Source "+alias+" is optional and error occured.");
                    response.add(createSearchResult(sourceAttributes));
                }
                return;
            }

            if (searchResponse.getTotalCount() == 0) {
                if (index == 0 || EntrySourceConfig.REQUIRED.equals(search)) {
                    if (debug) log.debug("Source "+alias+" is required and no results found.");

                } else {
                    if (debug) log.debug("Source "+alias+" is optional and no results found.");
                    response.add(createSearchResult(sourceAttributes));
                }
                return;
            }
/*
            do {
                SearchResult searchResult = searchResponse.next();

                SourceAttributes sa  = (SourceAttributes)sourceAttributes.clone();
                sa.set(alias, searchResult);

                expandSearchResult(
                        session, request, response, index, sa, requestedSources,
                        nextLinkedAttribute, prevAlias, prevLinkingAttribute
                );

            } while (searchResponse.hasNext());
*/
        }
    }

    public void expandSearchResult(
            Session session,
            SearchRequest request,
            SearchResponse response,
            int index,
            SourceAttributes sourceAttributes,
            Map<String,Boolean> requestedSources,
            String nextLinkedAttribute,
            String prevAlias,
            String prevLinkingAttribute
    ) throws Exception {

        String alias = getSearchOrder(index);

        int nextIndex = index+1;
        boolean lastSource = nextIndex == getSources().size();

        if (lastSource) {
            if (debug) log.debug("Source "+alias+" is the last source.");
            response.add(createSearchResult(sourceAttributes));
            return;
        }

        String nextAlias = getSearchOrder(nextIndex);

        EntrySource nextSource = getSource(nextAlias);
        String nextSearch = nextSource.getSearch();
        boolean nextRequested = requestedSources.get(nextAlias);

        if (EntrySourceConfig.IGNORE.equals(nextSearch)) {
            if (debug) log.debug("Source "+nextAlias+" is ignored.");
            response.add(createSearchResult(sourceAttributes));
            return;
        }

        if (EntrySourceConfig.OPTIONAL.equals(nextSearch) && !nextRequested) {
            if (debug) log.debug("Source "+nextAlias+" is optional and not requested.");
            response.add(createSearchResult(sourceAttributes));
            return;
        }

        if (nextLinkedAttribute == null || prevAlias == null || prevLinkingAttribute == null) {
            if (EntrySourceConfig.REQUIRED.equals(nextSearch)) {
                if (debug) log.debug("Source "+nextAlias+" is required and not linked.");

            } else {
                if (debug) log.debug("Source "+nextAlias+" is optional and not linked.");
                response.add(createSearchResult(sourceAttributes));
            }
            return;
        }

        Collection<Object> links = sourceAttributes.getValues(prevAlias, prevLinkingAttribute);

        if (links.isEmpty()) {
            if (EntrySourceConfig.REQUIRED.equals(nextSearch)) {
                if (debug) log.debug("Source "+nextAlias+" is required and not linked.");

            } else {
                if (debug) log.debug("Source "+nextAlias+" is optional and not linked.");
                response.add(createSearchResult(sourceAttributes));
            }
            return;
        }

        for (Object link : links) {

            if (debug) log.debug("Following link "+link+".");

            SearchRequest searchRequest = new SearchRequest();

            if ("dn".equals(nextLinkedAttribute)) {
                searchRequest.setDn((String)link);
                searchRequest.setScope(SearchRequest.SCOPE_BASE);

            } else {
                searchRequest.setFilter(new SimpleFilter(nextLinkedAttribute, "=", link));
            }

            expandSource(session, searchRequest, response, nextIndex, sourceAttributes, requestedSources);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        DN dn = session.getBindDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("DYNAMIC UNBIND", 70));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 70));
            log.debug(TextUtil.displayLine("DN    : "+dn, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

    }

    public Collection<EntrySourceConfig> getEffectiveSourceMappings() {
        Collection<EntrySourceConfig> list = new ArrayList<EntrySourceConfig>();
        list.addAll(entryConfig.getSourceConfigs());

        Entry parent = getParent();
        if (parent == null) {
            return list;
        }

        if (!(parent instanceof DynamicEntry)) {
            return list;
        }

        DynamicEntry dynamicParent = (DynamicEntry)parent;
        list.addAll(dynamicParent.getEffectiveSourceMappings());

        return list;
    }

    public List<Collection<EntrySource>> getGroupsOfSources() throws Exception {

        List<Collection<EntrySource>> results = new ArrayList<Collection<EntrySource>>();

        Collection<EntrySource> list = new ArrayList<EntrySource>();
        Connection lastConnection = null;

        for (Entry e : getPath()) {

            for (EntrySource sourceRef : e.getLocalSources()) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !connection.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<EntrySource>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public List<Collection<EntrySource>> getGroupsOfSources(
            SearchRequest request
    ) throws Exception {

        DN baseDn = request.getDn();

        if (getDn().matches(baseDn)) { // if (entry == base) {
            return getGroupsOfSources();
        }

        List<Collection<EntrySource>> results = new ArrayList<Collection<EntrySource>>();

        Collection<EntrySource> list = new ArrayList<EntrySource>();
        Connection lastConnection = null;

        for (Entry e : getRelativePath(baseDn)) {

            for (EntrySource sourceRef : e.getLocalSources()) {

                Source source = sourceRef.getSource();
                Connection connection = source.getConnection();

                if (lastConnection == null) {
                    lastConnection = connection;

                } else if (lastConnection != connection || !connection.isJoinSupported()) {
                    results.add(list);
                    list = new ArrayList<EntrySource>();
                    lastConnection = connection;
                }

                list.add(sourceRef);
            }
        }

        if (!list.isEmpty()) results.add(list);

        return results;
    }

    public Collection<DN> computeDns(Interpreter interpreter) throws Exception {

        Collection<DN> dns = new ArrayList<DN>();

        EntryAttributeConfig dnMapping = entryConfig.getAttributeConfig("dn");
        if (dnMapping != null) {
            String dnValue = (String)interpreter.eval(dnMapping);
            if (debug) log.debug("DN mapping: "+dnValue);
            dns.add(new DN(dnValue));
            return dns;
        }

        Collection<DN> parentDns = new ArrayList<DN>();

        Entry parent = getParent();
        if (parent != null) {
            if (parent instanceof DynamicEntry) {
                DynamicEntry dynamicParent = (DynamicEntry)parent;
                parentDns.addAll(dynamicParent.computeDns(interpreter));

            } else {
                parentDns.add(parent.getDn());
            }

        } else if (!getParentDn().isEmpty()) {
            parentDns.add(getParentDn());
        }

        if (parentDns.isEmpty()) {
            DN dn = getDn();
            if (debug) log.debug("DN: "+dn);
            dns.add(dn);

        } else {
            Collection<RDN> rdns = computeRdns(interpreter);

            DNBuilder db = new DNBuilder();

            for (RDN rdn : rdns) {
                //log.info("Processing RDN: "+rdn);

                for (DN parentDn : parentDns) {
                    //log.debug("Appending parent DN: "+parentDn);

                    db.set(rdn);
                    db.append(parentDn);
                    DN dn = db.toDn();

                    if (debug) log.debug("DN: " + dn);
                    dns.add(dn);
                }
            }
        }

        return dns;
    }

    public Collection<RDN> computeRdns(
            Interpreter interpreter
    ) throws Exception {

        //log.debug("Computing RDNs:");
        Attributes attributes = new Attributes();

        Collection<EntryAttributeConfig> rdnAttributes = getRdnAttributeConfigs();
        for (EntryAttributeConfig attributeMapping : rdnAttributes) {
            String name = attributeMapping.getName();

            Object value = interpreter.eval(attributeMapping);
            if (value == null) continue;

            attributes.addValue(name, value);
        }

        return TransformationUtil.convert(attributes);
    }

    public void extractSourceValues(
            DN baseDn,
            SourceAttributes sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting DN: "+baseDn);

        Interpreter interpreter = partition.newInterpreter();

        Entry base = this;
        
        while (base.getDn().getSize() > baseDn.getSize()) {
            base = base.getParent();
            if (base == null) return;
        }

        for (Entry entry : base.getPath()) {

            int i = baseDn.getSize() - entry.getDn().getSize();
            RDN rdn = baseDn.get(i);

            if (debug) {
                log.debug(" - RDN: "+rdn);
                //sourceValues.print();
            }

            if (!(entry instanceof DynamicEntry)) continue;

            DynamicEntry de = (DynamicEntry)entry;
            de.extractSourceValues(
                    rdn,
                    interpreter,
                    sourceValues
            );
        }
    }

    public void extractSourceValues(
            RDN rdn,
            Interpreter interpreter,
            SourceAttributes sourceValues
    ) throws Exception {

        interpreter.set(sourceValues);
        interpreter.set(rdn);
        interpreter.set("rdn", rdn);

        for (EntrySource sourceRef : getLocalSources()) {

            if (debug) log.debug("   - Source: "+sourceRef.getAlias()+" ("+sourceRef.getSource().getName()+")");

            extractSourceValues(
                    sourceRef,
                    interpreter,
                    sourceValues
            );
        }

        interpreter.clear();
    }

    public void extractSourceValues(
            EntrySource sourceRef,
            Interpreter interpreter,
            SourceAttributes sourceValues
    ) throws Exception {

        EntrySourceConfig sourceConfig = getSourceConfig(sourceRef.getAlias());

        Attributes attributes = sourceValues.get(sourceConfig.getAlias());

        for (EntryField fieldRef : sourceRef.getFields()) {
            String name = fieldRef.getName();

            for (EntryFieldConfig fieldMapping : sourceConfig.getFieldConfigs(name)) {
                Object value = interpreter.eval(fieldMapping);
                if (value == null) continue;

                if (FieldConfig.TYPE_INTEGER.equals(fieldRef.getType()) && value instanceof String) {
                    value = Integer.parseInt((String)value);
                }

                if (fieldMapping.isPrimaryKey()) {
                    String n = "primaryKey."+name;
                    attributes.addValue(n, value);
                    if (debug) log.debug("     - " + n + ": " + value);

                } else {
                    attributes.addValue(name, value);
                    if (debug) log.debug("     - " + name + ": " + value);
                }
            }
        }
    }

    public void propagate(
            SourceAttributes sourceValues
    ) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        propagate(sourceValues, interpreter);
    }

    public void propagate(
            SourceAttributes sourceValues,
            Interpreter interpreter
    ) throws Exception {

        List<Entry> path = getPath();

        for (Entry entry : path) {

            if (!(entry instanceof DynamicEntry)) continue;

            DynamicEntry dynamicEntry = (DynamicEntry)entry;
            
            Collection<EntrySourceConfig> sourceMappings = dynamicEntry.getSourceMappings();
            for (EntrySourceConfig sourceMapping : sourceMappings) {
                dynamicEntry.propagateSource(sourceMapping, sourceValues, interpreter);
            }
        }
    }

    public void propagateSource(
            EntrySourceConfig sourceConfig,
            SourceAttributes sourceValues,
            Interpreter interpreter
    ) throws Exception {

        if (debug) log.debug("Propagating source "+sourceConfig.getAlias()+" in "+getDn()+":");

        interpreter.set(sourceValues);

        Collection<EntryFieldConfig> fieldMappings = sourceConfig.getFieldConfigs();
        for (EntryFieldConfig fieldMapping : fieldMappings) {

            propagateField(
                    sourceConfig,
                    fieldMapping,
                    sourceValues,
                    interpreter
            );
        }

        interpreter.clear();
    }

    public void propagateField(
            EntrySourceConfig sourceConfig,
            EntryFieldConfig fieldMapping,
            SourceAttributes sourceValues,
            Interpreter interpreter
    ) throws Exception {

        String lsourceName = sourceConfig.getAlias();

        String lfieldName;
        if (fieldMapping.isPrimaryKey()) {
            lfieldName = "primaryKey."+fieldMapping.getName();
        } else {
            lfieldName = fieldMapping.getName();
        }

        String lhs = lsourceName + "." + lfieldName;

        Attributes lattributes = sourceValues.get(lsourceName);
        Attribute lattribute = lattributes.get(lfieldName);

        if (lattribute != null && !lattribute.isEmpty()) {
            if (debug) {
                for (Object value : lattribute.getValues()) {
                    //log.debug(" - "+lhs+" has been set to ["+value+"].");
                }
            }
            return;
        }

        Object value = interpreter.eval(fieldMapping);
        if (value == null) {
            //if (debug) log.debug(" - "+lhs+" is null.");
            return;
        }

        if (debug) log.debug(" - "+lhs+": "+value);

        if (value instanceof Collection) {
            lattributes.addValues(lfieldName, (Collection)value);
        } else {
            lattributes.addValue(lfieldName, value);
        }
    }

    public Object encrypt(String encryption, Object value) throws Exception {

        if (value instanceof Collection) {

            Collection list = (Collection)value;
            Collection<Object> newList = new HashSet<Object>();

            for (Object v : list) {

                if (debug) log.debug("Value: "+v+" ("+v.getClass()+")");

                String password = (String)v;
                if (password.startsWith("{")) {

                    int i = password.indexOf("}");
                    v = password.substring(i+1);

                } else {
                    v = new Password(encryption, password).encrypt();
                }

                newList.add(v);
            }

            value = newList;

        } else {

            if (debug) log.debug("Value: "+value+" ("+value.getClass()+")");

            String password = (String)value;
            if (password.startsWith("{")) {

                int i = password.indexOf("}");
                value = password.substring(i+1);

            } else {
                value = new Password(encryption, password).encrypt();
            }
        }

        return value;
    }
}