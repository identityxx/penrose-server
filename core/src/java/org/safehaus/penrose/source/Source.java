package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;

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

    public Source(Partition partition, SourceConfig sourceConfig) {
        this.partition = partition;
        this.sourceConfig = sourceConfig;

        this.name = sourceConfig.getName();
        this.parameters.putAll(sourceConfig.getParameters());

        Collection fieldConfigs = sourceConfig.getFieldConfigs();
        for (Iterator i=fieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
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
        return (String)parameters.get(name);
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
        return (Field)fields.get(fieldName);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        connection.add(this, request, response);
    }

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        connection.add(this, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            DN dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        connection.delete(this, request, response);
    }

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        connection.delete(this, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        connection.modify(this, request, response);
    }

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        connection.modify(this, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        connection.modrdn(this, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

        connection.search(this, request, response);

        return response;
    }

    public void search(
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        connection.search(this, request, response);
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

    public Object clone() {
        Source source = new Source(partition, sourceConfig);
        source.setConnection(connection);
        return source;
    }
}
