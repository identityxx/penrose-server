package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.FieldRef;
import org.safehaus.penrose.directory.FieldMapping;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.adapter.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Source implements Cloneable, SourceMBean {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected SourceConfig sourceConfig;
    protected SourceContext sourceContext;

    protected Map<String,Field> fields = new LinkedHashMap<String,Field>();
    protected Map<String,Field> fieldsByOriginalName = new LinkedHashMap<String,Field>();
    protected Collection<Field> primaryKeyFields = new ArrayList<Field>();

    public Source() {
    }

    public void init(SourceConfig sourceConfig, SourceContext sourceContext) throws Exception {

        log.debug("Initializing source "+sourceConfig.getName()+".");

        this.sourceConfig  = sourceConfig;
        this.sourceContext = sourceContext;

        Collection<FieldConfig> fieldConfigs = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fieldConfigs) {
            Field field = new Field(this, fieldConfig);
            addField(field);
        }

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public String getDescription() {
        return sourceConfig.getDescription();
    }
    
    public void addField(Field field) {
        fields.put(field.getName().toLowerCase(), field);
        fieldsByOriginalName.put(field.getOriginalName().toLowerCase(), field);

        if (field.isPrimaryKey()) primaryKeyFields.add(field);
    }

    public String getName() {
        return sourceConfig.getName();
    }

    public String getConnectionName() {
        return sourceConfig.getConnectionName();
    }

    public Connection getConnection() {
        return sourceContext.getConnection();
    }

    public void setName(String name) {
        sourceConfig.setName(name);
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public Partition getPartition() {
        return sourceContext.getPartition();
    }

    public String getParameter(String name) {
        return sourceConfig.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return sourceConfig.getParameterNames();
    }
    
    public Map<String,String> getParameters() {
        return sourceConfig.getParameters();
    }

    public void setParameter(String name, String value) {
        sourceConfig.setParameter(name, value);
    }

    public Collection<String> getPrimaryKeyNames() {
        return sourceConfig.getPrimaryKeyNames();
    }

    public Collection<Field> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public Field getPrimaryKeyField() {
        return primaryKeyFields.iterator().next();
    }

    public Collection<Field> getFields() {
        return fields.values();
    }

    public Field getField(String fieldName) {
        return fields.get(fieldName.toLowerCase());
    }

    public Field getFieldByOriginalName(String fieldOriginalName) {
        return fieldsByOriginalName.get(fieldOriginalName.toLowerCase());
    }

    public SourceContext getSourceContext() {
        return sourceContext;
    }

    public void setSourceContext(SourceContext sourceContext) {
        this.sourceContext = sourceContext;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(
            String dn,
            Attributes attributes
    ) throws Exception {
        return add(new DN(dn), attributes);
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {
        return add(new DN(rdn), attributes);
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        add(request, response);

        return response;
    }

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        add(null, request, response);
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) log.debug("Adding "+request.getDn());

        //connection.add(session, this, request, response);
    }

    public void add(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();
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

        add(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public BindResponse bind(
            String dn,
            byte[] password
    ) throws Exception {
        return bind(new DN(dn), password);
    }

    public BindResponse bind(
            RDN rdn,
            byte[] password
    ) throws Exception {
        return bind(new DN(rdn), password);
    }

    public BindResponse bind(
            DN dn,
            byte[] password
    ) throws Exception {

        BindRequest request = new BindRequest();
        request.setDn(dn);
        request.setPassword(password);

        BindResponse response = new BindResponse();

        bind(request, response);

        return response;
    }

    public void bind(
            BindRequest request,
            BindResponse response
    ) throws Exception {

        bind(null, request, response);
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        if (debug) log.debug("Binding "+request.getDn());

        throw LDAP.createException(LDAP.LDAP_NOT_SUPPORTED);
    }

    public void bind(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();
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

        bind(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        if (debug) log.debug("Compare "+request.getDn());

        //connection.compare(null, this, request, response);
    }

    public void compare(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();
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

        compare(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            String dn
    ) throws Exception {
        return delete(new DN(dn));
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {
        return delete(new DN(rdn));
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        delete(request, response);

        return response;
    }

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        delete(null, request, response);
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) log.debug("Deleting "+request.getDn());

        //connection.delete(null, this, request, response);
    }

    public void delete(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();
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

        delete(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(String dn) throws Exception {
        return find(new DN(dn));
    }

    public SearchResult find(RDN rdn) throws Exception {
        return find(new DN(rdn));
    }

    public SearchResult find(DN dn) throws Exception {
        return find(null, dn);
    }

    public SearchResult find(Session session, String dn) throws Exception {
        return find(session, new DN(dn));
    }

    public SearchResult find(Session session, RDN rdn) throws Exception {
        return find(session, new DN(rdn));
    }

    public SearchResult find(Session session, DN dn) throws Exception {

        if (debug) log.debug("Finding "+dn);

        SearchResponse response = search(session, dn, null, SearchRequest.SCOPE_BASE);
        if (!response.hasNext()) return null;

        return response.next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {
        return modify(new DN(dn), modifications);
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {
        return modify(new DN(rdn), modifications);
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        modify(request, response);

        return response;
    }

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        modify(null, request, response);
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) log.debug("Modifying "+request.getDn());

        //connection.modify(null, this, request, response);
    }

    public void modify(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();
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

        modify(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return modrdn(new DN(dn), new RDN(newRdn), deleteOldRdn);
    }

    public ModRdnResponse modrdn(
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return modrdn(new DN(rdn), newRdn, deleteOldRdn);
    }

    public ModRdnResponse modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnRequest request = new ModRdnRequest();
        request.setDn(dn);
        request.setNewRdn(newRdn);
        request.setDeleteOldRdn(deleteOldRdn);

        ModRdnResponse response = new ModRdnResponse();

        modrdn(request, response);

        return response;
    }

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        modrdn(null, request, response);
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) log.debug("Renaming "+request.getDn());

        //connection.modrdn(null, this, request, response);
    }

    public void modrdn(
            Session session,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();
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

        modrdn(session, newRequest, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            String dn,
            String filter,
            int scope
    ) throws Exception {
        return search(new DN(dn), FilterTool.parseFilter(filter), scope);
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            int scope
    ) throws Exception {
        return search(new DN(rdn), filter, scope);
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {
        return search(null, dn, filter, scope);
    }

    public SearchResponse search(
            Session session,
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        search(session, request, response);

        return response;
    }

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        search(null, request, response);
    }

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) log.debug("Searching "+request.getDn());
        
        //connection.search(session, this, request, response);
    }

    public void search(
            final Session session,
            //final Collection<SourceRef> primarySourceRefs,
            final Collection<SourceRef> localSourceRefs,
            final Collection<SourceRef> sourceRefs,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final SourceRef sourceRef = sourceRefs.iterator().next();

        Interpreter interpreter = getPartition().newInterpreter();

        FilterBuilder filterBuilder = new FilterBuilder(
                getPartition(),
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

        search(session, newRequest, newResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Table
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create() throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void rename(Source newSource) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void drop() throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void clear() throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void status() throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public long getCount() throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Clone
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Object clone() throws CloneNotSupportedException {
        
        Source source = (Source)super.clone();

        source.sourceConfig         = (SourceConfig)sourceConfig.clone();
        source.sourceContext        = sourceContext;

        source.fields               = new LinkedHashMap<String,Field>();
        source.fieldsByOriginalName = new LinkedHashMap<String,Field>();
        source.primaryKeyFields     = new ArrayList<Field>();

        for (Field field : fields.values()) {
            source.addField((Field)field.clone());
        }

        return source;
    }
}
