package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.SourceValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Source implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected SourceConfig sourceConfig;
    protected SourceContext sourceContext;

    protected Partition partition;
    protected Connection connection;

    protected Map<String,Field> fields = new LinkedHashMap<String,Field>();
    protected Collection<Field> primaryKeyFields = new ArrayList<Field>();
    protected Collection<Field> indexFields = new ArrayList<Field>();

    public Source() {
    }

    public void init(SourceConfig sourceConfig, SourceContext sourceContext) {

        log.debug("Initializing source "+sourceConfig.getName()+".");

        this.sourceConfig  = sourceConfig;
        this.sourceContext = sourceContext;

        this.partition     = sourceContext.getPartition();
        this.connection    = sourceContext.getConnection();

        Collection<FieldConfig> fieldConfigs = sourceConfig.getFieldConfigs();
        for (FieldConfig fieldConfig : fieldConfigs) {
            Field field = new Field(this, fieldConfig);
            addField(field);
        }
    }

    public void destroy() throws Exception {
    }

    public void addField(Field field) {
        fields.put(field.getName(), field);

        if (field.isPrimaryKey()) primaryKeyFields.add(field);
        if (field.isIndex()) indexFields.add(field);
    }

    public String getName() {
        return sourceConfig.getName();
    }

    public String getConnectionName() {
        return sourceConfig.getConnectionName();
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

    public Collection<String> getIndexFieldNames() {
        return sourceConfig.getIndexFieldNames();
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
            RDN rdn,
            Attributes attributes
    ) throws Exception {
        add(new DN(rdn), attributes);
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
            RDN rdn
    ) throws Exception {
        delete(new DN(rdn));
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
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(String dn) throws Exception {
        return find(new DN(dn));
    }

    public SearchResult find(RDN rdn) throws Exception {
        return find(new DN(rdn));
    }

    public SearchResult find(DN dn) throws Exception {
        SearchResponse response = search(dn, null, SearchRequest.SCOPE_BASE);

        if (!response.hasNext()) return null;

        return response.next();
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
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {
        modify(new DN(rdn), modifications);
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
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        modrdn(new DN(rdn), newRdn, deleteOldRdn);
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

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        search(request, response);

        return response;
    }

    public void search(
            SearchRequest request,
            SearchResponse response
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

    public void clear() throws Exception {
        connection.clear(this);
    }

    public void status() throws Exception {
        connection.status(this);
    }

    public long getCount() throws Exception {
        return connection.getCount(this);
    }

    public SourceContext getSourceContext() {
        return sourceContext;
    }

    public void setSourceContext(SourceContext sourceContext) {
        this.sourceContext = sourceContext;
    }

    public Object clone() throws CloneNotSupportedException {
        
        Source source = (Source)super.clone();

        source.sourceConfig     = (SourceConfig)sourceConfig.clone();
        source.sourceContext    = sourceContext;

        source.partition        = partition;
        source.connection       = connection;

        source.fields           = new LinkedHashMap<String,Field>();
        source.primaryKeyFields = new ArrayList<Field>();
        source.indexFields      = new ArrayList<Field>();

        for (Field field : fields.values()) {
            source.addField((Field)field.clone());
        }

        return source;
    }
}
