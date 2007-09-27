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
package org.safehaus.penrose.adapter;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.FieldMapping;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.FieldRef;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata 
 */
public abstract class Adapter {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected AdapterConfig adapterConfig;
    protected AdapterContext adapterContext;

    protected Partition partition;
    protected Connection connection;

    public void init(AdapterConfig adapterConfig, AdapterContext adapterContext) throws Exception {
        this.adapterConfig = adapterConfig;
        this.adapterContext = adapterContext;

        partition = adapterContext.getPartition();
        connection = adapterContext.getConnection();

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public boolean isJoinSupported() {
        return false;
    }

    public String getSyncClassName() {
        return SourceSync.class.getName();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Table
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create(Source source) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void rename(Source oldSource, Source newSource) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void drop(Source source) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void clear(Source source) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void status(Source source) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public long getCount(Source source) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Source source,
            AddRequest request,
            AddResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void add(
            Session session,
            Entry entry,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        Attributes attributes = request.getAttributes();
        for (Attribute attribute : attributes.getAll()) {
            String attributeName = attribute.getName();
            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(attributeName, attributeValue);
        }

        Attributes sv = sourceValues.get(sourceRef.getAlias());
        Attributes newAttributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();

        if (debug) log.debug("Target values:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            
            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.ADD)) continue;

            Field field = fieldRef.getField();

            Attribute attribute = sv == null ? null : sv.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) {
                value = interpreter.eval(fieldRef);
            }

            if (value == null) continue;

            String fieldName = field.getOriginalName();
            if (debug) log.debug(" - " + fieldName + ": " + value);

            newAttributes.addValue(fieldName, value);
            if (field.isPrimaryKey()) rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());
        log.debug("Target DN: "+dn);

        AddRequest newRequest = new AddRequest(request);
        newRequest.setDn(dn);
        newRequest.setAttributes(newAttributes);

        add(session, source, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
    }

    public void bind(
            Session session,
            Entry entry,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);
            interpreter.set(attributeName, attributeValue);
        }

        Attributes sv = sourceValues.get(sourceRef.getAlias());
        RDNBuilder rb = new RDNBuilder();

        if (debug) log.debug("Target values:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            if (!fieldRef.isPrimaryKey()) continue;

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.BIND)) continue;

            Field field = fieldRef.getField();

            Attribute attribute = sv == null ? null : sv.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) {
                value = interpreter.eval(fieldRef);
            }

            if (value == null) continue;

            String fieldName = field.getOriginalName();
            if (debug) log.debug(" - "+fieldName+": "+value);

            rb.set(fieldName, value);
        }

        if (rb.isEmpty()) {
            log.error("Empty RDN.");
            throw LDAP.createException(LDAP.OPERATIONS_ERROR);
        }

        DN dn = new DN(rb.toRdn());

        BindRequest newRequest = new BindRequest(request);
        newRequest.setDn(dn);

        bind(session, source, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Source source,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void compare(
            Session session,
            Entry entry,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        Attributes sv = sourceValues.get(sourceRef.getAlias());
        RDNBuilder rb = new RDNBuilder();

        if (debug) log.debug("Target values:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            if (!fieldRef.isPrimaryKey()) continue;

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.DELETE)) continue;

            Field field = fieldRef.getField();

            Attribute attribute = sv == null ? null : sv.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) {
                value = interpreter.eval(fieldRef);
            }

            if (value == null) continue;

            String fieldName = field.getOriginalName();
            if (debug) log.debug(" - "+fieldName+": "+value);

            rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());

        CompareRequest newRequest = (CompareRequest)request.clone();
        newRequest.setDn(dn);

        interpreter.clear();
        interpreter.set(request.getAttributeName(), request.getAttributeValue());

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.COMPARE)) continue;

            Object value = interpreter.eval(fieldRef);
            if (value == null) continue;

            if (value instanceof Collection) {
                Collection list = (Collection)value;
                value = list.iterator().next();
            }

            String fieldName = fieldRef.getOriginalName();

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

        compare(session, source, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void delete(
            Session session,
            Entry entry,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        Attributes sv = sourceValues.get(sourceRef.getAlias());
        RDNBuilder rb = new RDNBuilder();

        if (debug) log.debug("Target values:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            if (!fieldRef.isPrimaryKey()) continue;

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.DELETE)) continue;

            Field field = fieldRef.getField();

            Attribute attribute = sv == null ? null : sv.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) {
                value = interpreter.eval(fieldRef);
            }

            if (value == null) continue;

            String fieldName = field.getOriginalName();
            if (debug) log.debug(" - "+fieldName+": "+value);
            
            rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());

        DeleteRequest newRequest = new DeleteRequest(request);
        newRequest.setDn(dn);

        delete(session, source, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void modify(
            Session session,
            Entry entry,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        Attributes sv = sourceValues.get(sourceRef.getAlias());
        RDNBuilder rb = new RDNBuilder();

        if (debug) log.debug("Target values:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            if (!fieldRef.isPrimaryKey()) continue;

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.MODIFY)) continue;

            Field field = fieldRef.getField();

            Attribute attribute = sv == null ? null : sv.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) {
                value = interpreter.eval(fieldRef);
            }

            if (value == null) continue;

            String fieldName = field.getOriginalName();
            if (debug) log.debug(" - "+fieldName+": "+value);

            rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());

        Collection<Modification> newModifications = new ArrayList<Modification>();

        Collection<Modification> modifications = request.getModifications();
        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

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
                    for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                        Collection<String> operations = fieldRef.getOperations();
                        if (!operations.isEmpty() && !operations.contains(FieldMapping.MODIFY)) continue;

                        String fieldName = fieldRef.getName();
                        if (fieldRef.isPrimaryKey()) continue;

                        Object value = interpreter.eval(fieldRef);
                        if (value == null) continue;

                        if (debug) log.debug(" => Replacing field " + fieldName + ": " + value);

                        Attribute newAttribute = new Attribute(fieldRef.getOriginalName());
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
                    for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                        Collection<String> operations = fieldRef.getOperations();
                        if (!operations.isEmpty() && !operations.contains(FieldMapping.MODIFY)) continue;

                        String fieldName = fieldRef.getName();

                        String variable = fieldRef.getVariable();
                        if (variable == null) {
                            Object value = interpreter.eval(fieldRef);
                            if (value == null) continue;

                            if (debug) log.debug(" ==> Deleting field " + fieldName + ": "+value);

                            Attribute newAttribute = new Attribute(fieldRef.getOriginalName());
                            newAttribute.addValue(value);
                            newModifications.add(new Modification(type, newAttribute));

                        } else {
                            if (!variable.equals(attributeName)) continue;

                            if (debug) log.debug(" ==> Deleting field " + fieldName + ": "+attributeValues);

                            Attribute newAttribute = new Attribute(fieldRef.getOriginalName());
                            for (Object value : attributeValues) {
                                newAttribute.addValue(value);
                            }
                            newModifications.add(new Modification(type, newAttribute));
                        }

                    }
                    break;
            }
        }

        ModifyRequest newRequest = new ModifyRequest();
        newRequest.setDn(dn);
        newRequest.setModifications(newModifications);

        modify(session, source, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void modrdn(
            Session session,
            Entry entry,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (String attributeName : rdn.getNames()) {
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        Attributes sv = sourceValues.get(sourceRef.getAlias());
        RDNBuilder rb = new RDNBuilder();

        if (debug) log.debug("Target values:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            if (!fieldRef.isPrimaryKey()) continue;

            Collection<String> operations = fieldRef.getOperations();
            if (!operations.isEmpty() && !operations.contains(FieldMapping.MODRDN)) continue;

            Field field = fieldRef.getField();

            Attribute attribute = sv == null ? null : sv.get(field.getName());
            Object value = attribute == null ? null : attribute.getValue();

            if (value == null) {
                value = interpreter.eval(fieldRef);
            }

            if (value == null) continue;

            String fieldName = field.getOriginalName();
            if (debug) log.debug(" - "+fieldName+": "+value);

            rb.set(fieldName, value);
        }

        DN dn = new DN(rb.toRdn());

        interpreter.clear();
        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (String attributeName : newRdn.getNames()) {
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        rb.clear();

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            if (!fieldRef.isPrimaryKey()) continue;

            Object value = interpreter.eval(fieldRef);
            if (value == null) continue;

            Field field = fieldRef.getField();
            rb.set(field.getOriginalName(), value);
        }

        ModRdnRequest newRequest = new ModRdnRequest(request);
        newRequest.setDn(dn);
        newRequest.setNewRdn(rb.toRdn());

        modrdn(session, source, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Source source,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void search(
            final Session session,
            final Collection<SourceRef> primarySourceRefs,
            final Collection<SourceRef> localSourceRefs,
            final Collection<SourceRef> sourceRefs,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final SourceRef sourceRef = sourceRefs.iterator().next();
        Source source = sourceRef.getSource();

        Interpreter interpreter = partition.newInterpreter();

        FilterBuilder filterBuilder = new FilterBuilder(
                partition,
                sourceRefs,
                sourceValues,
                interpreter
        );

        Filter filter = filterBuilder.getFilter();
        if (debug) log.debug("Base filter: "+filter);

        filterBuilder.append(request.getFilter());
        filter = filterBuilder.getFilter();
        if (debug) log.debug("Added search filter: "+filter);

        SearchRequest newRequest = (SearchRequest)request.clone();
        newRequest.setFilter(filter);

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                SearchResult searchResult = new SearchResult();
                searchResult.setDn(result.getDn());

                SourceValues sourceValues = new SourceValues();
                sourceValues.set(sourceRef.getAlias(), result.getAttributes());
                searchResult.setSourceValues(sourceValues);

                response.add(searchResult);
            }
            public void close() throws Exception {
                response.close();
            }
        };

        search(session, source, newRequest, newResponse);
    }

    public Object openConnection() throws Exception {
        return null;
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getParameter(String name) {
        return connection.getParameter(name);
    }

    public Map<String,String> getParameters() {
        return connection.getParameters();
    }

    public Collection getParameterNames() {
        return connection.getParameterNames();
    }

    public String removeParameter(String name) {
        return connection.removeParameter(name);
    }

    public String getAdapterName() {
        return adapterConfig.getName();
    }

    public String getConnectionName() {
        return connection.getConnectionName();
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public AdapterContext getAdapterContext() {
        return adapterContext;
    }

    public void setAdapterContext(AdapterContext adapterContext) {
        this.adapterContext = adapterContext;
    }
}
