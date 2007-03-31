package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Source {

    private Partition partition;
    private SourceConfig sourceConfig;
    private Connection connection;

    private Collection primaryKeyNames;
    Collection primaryKeyFields = new ArrayList();
    Map fields = new LinkedHashMap();

    public Source(Partition partition, SourceConfig sourceConfig) {
        this.partition = partition;
        this.sourceConfig = sourceConfig;

        Collection fieldConfigs = sourceConfig.getFieldConfigs();
        for (Iterator i=fieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            String fieldName = fieldConfig.getName();

            Field field = new Field(fieldConfig);
            fields.put(fieldName, field);

            if (fieldConfig.isPrimaryKey()) primaryKeyFields.add(field);
        }

        primaryKeyNames = sourceConfig.getPrimaryKeyNames();
    }

    public String getName() {
        return sourceConfig.getName();
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
        return sourceConfig.getParameter(name);
    }

    public Collection getPrimaryKeyNames() {
        return primaryKeyNames;
    }

    public void setPrimaryKeyNames(Collection primaryKeyNames) {
        this.primaryKeyNames = primaryKeyNames;
    }

    public Collection getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public Collection getFields() {
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

    public SearchResponse search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        connection.search(this, request, response);

        return response;
    }

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        connection.search(this, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Table
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void create() throws Exception {
        connection.create(this);
    }

    public void drop() throws Exception {
        connection.drop(this);
    }
}
