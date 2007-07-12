package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.entry.SourceValues;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Source implements Cloneable {

    protected String name;
    protected Map<String,String> parameters = new LinkedHashMap<String,String>();

    protected Partition partition;
    protected SourceConfig sourceConfig;
    protected Connection connection;

    protected Map<String,Field> fields = new LinkedHashMap<String,Field>();

    protected Collection<String> primaryKeyNames = new ArrayList<String>();
    protected Collection<Field> primaryKeyFields = new ArrayList<Field>();

    protected Collection<String> indexFieldNames = new ArrayList<String>();
    protected Collection<Field> indexFields = new ArrayList<Field>();

    public Source() {
    }

    public Source(Partition partition, SourceConfig sourceConfig) {
        this.partition = partition;
        this.sourceConfig = sourceConfig;

        this.name = sourceConfig.getName();
        this.parameters.putAll(sourceConfig.getParameters());

        Collection<FieldConfig> fieldConfigs = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fieldConfigs) {
            String fieldName = fieldConfig.getName();

            Field field = new Field(fieldConfig);
            fields.put(fieldName, field);

            if (fieldConfig.isPrimaryKey()) primaryKeyFields.add(field);
            if (fieldConfig.isIndex()) indexFields.add(field);
        }

        primaryKeyNames.addAll(sourceConfig.getPrimaryKeyNames());
        indexFieldNames.addAll(sourceConfig.getIndexFieldNames());
    }

    public String getName() {
        return name;
    }

    public String getConnectionName() {
        return sourceConfig.getConnectionName();
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public Collection<String> getPrimaryKeyNames() {
        return primaryKeyNames;
    }

    public void setPrimaryKeyNames(Collection<String> primaryKeyNames) {
        if (this.primaryKeyNames == primaryKeyNames) return;
        this.primaryKeyNames.clear();
        this.primaryKeyNames.addAll(primaryKeyNames);
    }

    public Collection<Field> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public Collection<String> getIndexFieldNames() {
        return indexFieldNames;
    }

    public void setIndexFieldNames(Collection<String> indexFieldNames) {
        if (this.indexFieldNames == indexFieldNames) return;
        this.indexFieldNames.clear();
        this.indexFieldNames.addAll(indexFieldNames);
    }

    public Collection<Field> getIndexFields() {
        return indexFields;
    }

    public void setIndexFields(Collection<Field> indexFields) {
        if (this.indexFields == indexFields) return;
        this.indexFields.clear();
        this.indexFields.addAll(indexFields);
    }

    public Collection<Field> getFields() {
        return fields.values();
    }

    public Field getField(String fieldName) {
        return fields.get(fieldName);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            String dn,
            Attributes attributes
    ) throws Exception {
        add(new DN(dn), attributes);
    }

    public void add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        add(request, response);
    }

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SourceValues sourceValues = new SourceValues();
        connection.add(null, this, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            String dn
    ) throws Exception {
        delete(new DN(dn));
    }

    public void delete(
            DN dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        delete(request, response);
    }

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SourceValues sourceValues = new SourceValues();
        connection.delete(null, this, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {
        modify(new DN(dn), modifications);
    }

    public void modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        modify(request, response);
    }

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SourceValues sourceValues = new SourceValues();
        connection.modify(null, this, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        modrdn(new DN(dn), new RDN(newRdn), deleteOldRdn);
    }

    public void modrdn(
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
    }

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SourceValues sourceValues = new SourceValues();
        connection.modrdn(null, this, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse<SearchResult> search(
            String dn,
            String filter,
            int scope
    ) throws Exception {
        return search(new DN(dn), FilterTool.parseFilter(filter), scope);
    }

    public SearchResponse<SearchResult> search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse<SearchResult> response = new SearchResponse<SearchResult>();

        search(request, response);

        return response;
    }

    public void search(
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        SourceValues sourceValues = new SourceValues();
        connection.search(null, this, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Table
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create() throws Exception {
        connection.create(this);
    }

    public void rename(Source newSource) throws Exception {
        connection.rename(this, newSource);
    }

    public void drop() throws Exception {
        connection.drop(this);
    }

    public void clean() throws Exception {
        connection.clean(this);
    }

    public void status() throws Exception {
        connection.status(this);
    }

    public void copy(Source source) {
        name = source.name;

        parameters.clear();
        parameters.putAll(source.parameters);

        partition = source.partition;
        sourceConfig = source.sourceConfig;
        connection = source.connection;

        fields.clear();
        fields.putAll(source.fields);

        primaryKeyNames.clear();
        primaryKeyNames.addAll(source.primaryKeyNames);

        primaryKeyFields.clear();
        primaryKeyFields.addAll(source.primaryKeyFields);

        indexFieldNames.clear();
        indexFieldNames.addAll(source.indexFieldNames);

        indexFields.clear();
        indexFields.addAll(source.indexFields);
    }

    public Object clone() throws CloneNotSupportedException {
        Source source = (Source)super.clone();
        source.copy(this);
        return source;
    }
}
