package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Source implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();
    public boolean warn = log.isWarnEnabled();

    protected SourceConfig sourceConfig;
    protected SourceContext sourceContext;

    protected Partition partition;

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

        partition = sourceContext.getPartition();

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

    public String getAdapterName() {
        return sourceContext.getAdapter().getName();
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

    public Partition getPartition(String name) {
        return sourceContext.getPartition().getPartitionContext().getPartition(name);
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

    public Collection<String> getFieldNames() {
        return sourceConfig.getFieldNames();
    }

    public Collection<String> getFieldOriginalNames() {
        return sourceConfig.getFieldOriginalNames();
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
            Session session,
            String dn,
            Attributes attributes
    ) throws Exception {
        return add(session, new DN(dn), attributes);
    }

    public AddResponse add(
            Session session,
            RDN rdn,
            Attributes attributes
    ) throws Exception {
        return add(session, new DN(rdn), attributes);
    }

    public AddResponse add(
            Session session,
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        add(session, request, response);

        return response;
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) log.debug("Adding "+request.getDn()+".");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public BindResponse bind(
            Session session,
            String dn,
            byte[] password
    ) throws Exception {
        return bind(session, new DN(dn), password);
    }

    public BindResponse bind(
            Session session,
            RDN rdn,
            byte[] password
    ) throws Exception {
        return bind(session, new DN(rdn), password);
    }

    public BindResponse bind(
            Session session,
            DN dn,
            byte[] password
    ) throws Exception {

        BindRequest request = new BindRequest();
        request.setDn(dn);
        request.setPassword(password);

        BindResponse response = new BindResponse();

        bind(session, request, response);

        return response;
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {
        bind(session, request, response, null);
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response,
            Attributes attributes
    ) throws Exception {

        if (debug) log.debug("Binding as "+request.getDn()+".");

        throw LDAP.createException(LDAP.LDAP_NOT_SUPPORTED);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        if (debug) log.debug("Compare "+request.getDn()+".");

        //connection.compare(null, this, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            Session session,
            String dn
    ) throws Exception {
        return delete(session, new DN(dn));
    }

    public DeleteResponse delete(
            Session session,
            RDN rdn
    ) throws Exception {
        return delete(session, new DN(rdn));
    }

    public DeleteResponse delete(
            Session session,
            DN dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        delete(session, request, response);

        return response;
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) log.debug("Deleting "+request.getDn()+".");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Session createAdminSession() throws Exception {
        SessionManager sessionManager = getPartition().getPartitionContext().getSessionManager();
        return sessionManager.createAdminSession();
    }

    public SearchResult find(DN dn) throws Exception {

        Session session = createAdminSession();

        try {
            return find(session, dn);

        } finally {
            session.close();
        }
    }

    public SearchResult find(Session session, String dn) throws Exception {
        return find(session, new DN(dn));
    }

    public SearchResult find(Session session, RDN rdn) throws Exception {
        return find(session, new DN(rdn));
    }

    public SearchResult find(Session session, DN dn) throws Exception {

        if (debug) log.debug("Finding "+dn+".");

        SearchResponse response = search(session, dn, null, SearchRequest.SCOPE_BASE);
        if (!response.hasNext()) return null;

        return response.next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            Session session,
            String dn,
            Collection<Modification> modifications
    ) throws Exception {
        return modify(session, new DN(dn), modifications);
    }

    public ModifyResponse modify(
            Session session,
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {
        return modify(session, new DN(rdn), modifications);
    }

    public ModifyResponse modify(
            Session session,
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        modify(session, request, response);

        return response;
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) log.debug("Modifying "+request.getDn()+".");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(
            Session session,
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return modrdn(session, new DN(dn), new RDN(newRdn), deleteOldRdn);
    }

    public ModRdnResponse modrdn(
            Session session,
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return modrdn(session, new DN(rdn), newRdn, deleteOldRdn);
    }

    public ModRdnResponse modrdn(
            Session session,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnRequest request = new ModRdnRequest();
        request.setDn(dn);
        request.setNewRdn(newRdn);
        request.setDeleteOldRdn(deleteOldRdn);

        ModRdnResponse response = new ModRdnResponse();

        modrdn(session, request, response);

        return response;
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) log.debug("Renaming "+request.getDn()+".");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            Session session,
            String dn,
            String filter,
            int scope
    ) throws Exception {
        return search(session, new DN(dn), FilterTool.parseFilter(filter), scope);
    }

    public SearchResponse search(
            Session session,
            RDN rdn,
            Filter filter,
            int scope
    ) throws Exception {
        return search(session, new DN(rdn), filter, scope);
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
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (debug) log.debug("Searching "+request.getDn()+".");
        response.close();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        if (debug) log.debug("Unbinding as "+request.getDn()+".");

        throw LDAP.createException(LDAP.LDAP_NOT_SUPPORTED);
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

    public void clear(Session session) throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public void status() throws Exception {
        throw LDAP.createException(LDAP.OPERATIONS_ERROR);
    }

    public long getCount(Session session) throws Exception {
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
