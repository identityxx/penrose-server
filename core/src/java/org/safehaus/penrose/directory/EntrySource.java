package org.safehaus.penrose.directory;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.mapping.Mapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class EntrySource implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected Entry entry;
    protected EntrySourceConfig sourceConfig;
    protected Source source;

    protected String alias;
    protected boolean primarySourceRef;

    protected Map<String,Collection<EntryField>> fields = new LinkedHashMap<String,Collection<EntryField>>();
    protected Map<String, EntryField> primaryKeyFields = new LinkedHashMap<String, EntryField>();

    protected String add;
    protected String bind;
    protected String delete;
    protected String modify;
    protected String modrdn;
    protected String search;

    public EntrySource(Source source) throws Exception {
        this.source = source;
        this.alias = source.getName();

        //if (debug) log.debug("Source ref "+source.getName()+" "+alias+":");

        for (Field field : source.getFields()) {
            //if (debug) log.debug(" - field "+field.getName());

            EntryField entryField = new EntryField(this, field);
            addField(entryField);
        }
    }

    public EntrySource(Entry entry, EntrySourceConfig sourceConfig, Source source) throws Exception {
        this.entry = entry;
        this.sourceConfig = sourceConfig;
        this.source = source;
        this.alias = sourceConfig.getAlias();

        String alias = getAlias();

        String primarySourceName = entry.getPrimarySourceAlias();
        this.primarySourceRef = alias.equals(primarySourceName);

        if (debug) log.debug("Source ref "+source.getName()+" "+alias+":");

        Collection<EntryFieldConfig> fieldMappings = sourceConfig.getFieldConfigs();
        for (EntryFieldConfig fieldMapping : fieldMappings) {
            String fieldName = fieldMapping.getName();
            if (debug) log.debug(" - field "+fieldName);

            Field field = source.getField(fieldName);
            if (field == null) throw new Exception("Unknown field: " + fieldName);

            EntryField entryField = new EntryField(entry, this, field, fieldMapping);
            addField(entryField);
        }

        add = sourceConfig.getAdd();
        bind = sourceConfig.getBind();
        delete = sourceConfig.getDelete();
        modify = sourceConfig.getModify();
        modrdn = sourceConfig.getModrdn();
        search = sourceConfig.getSearch();
    }

    public void addField(EntryField field) {
        String fieldName = field.getName();
        Collection<EntryField> list = fields.get(fieldName);
        if (list == null) {
            list = new ArrayList<EntryField>();
            fields.put(fieldName, list);
        }
        list.add(field);

        if (field.isPrimaryKey()) primaryKeyFields.put(fieldName, field);
    }

    public Collection<EntryField> getPrimaryKeyFields() {
        return primaryKeyFields.values();
    }

    public EntryField getPrimaryKeyField(String fieldName) {
        return primaryKeyFields.get(fieldName);
    }

    public Collection<EntryField> getFields() {
        Collection<EntryField> results = new ArrayList<EntryField>();
        for (Collection<EntryField> list : fields.values()) {
            results.addAll(list);
        }
        return results;
    }

    public EntryField getField(String fieldName) {
        Collection<EntryField> results = getFields(fieldName);
        if (results.isEmpty()) return null;
        return results.iterator().next();
    }

    public Collection<EntryField> getFields(String fieldName) {
        Collection<EntryField> results = new ArrayList<EntryField>();
        Collection<EntryField> list = fields.get(fieldName);
        if (list == null || list.isEmpty()) return results;
        results.addAll(list);
        return results;
    }

    public String getAlias() {
        return alias == null ? source.getName() : alias;
    }

    public String getSearch() {
        return search;
    }

    public String toString() {
        return getAlias();
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;

        for (EntryField entryField : getFields()) {
            Field field = entryField.getField();
            entryField.setField(field);
        }
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    public Collection<String> getParameterNames() {
        return source.getParameterNames();
    }

    public String getParameter(String name) {
        return source.getParameter(name);
    }

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public String getAdd() {
        return add;
    }

    public void setAdd(String add) {
        this.add = add;
    }

    public String getDelete() {
        return delete;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }

    public String getModify() {
        return modify;
    }

    public void setModify(String modify) {
        this.modify = modify;
    }

    public String getModrdn() {
        return modrdn;
    }

    public void setModrdn(String modrdn) {
        this.modrdn = modrdn;
    }

    public int hashCode() {
        return (entry == null ? 0 : entry.hashCode()) +
                (sourceConfig == null ? 0 : sourceConfig.hashCode()) +
                (source == null ? 0 : source.hashCode());
    }

    public Object clone() throws CloneNotSupportedException {

        EntrySource entrySource = (EntrySource)super.clone();

        entrySource.entry = entry;
        entrySource.sourceConfig = sourceConfig;
        entrySource.source = source;

        entrySource.alias = alias;
        entrySource.primarySourceRef = primarySourceRef;

        entrySource.fields = new LinkedHashMap<String,Collection<EntryField>>();
        entrySource.primaryKeyFields = new LinkedHashMap<String, EntryField>();

        for (EntryField entryField : getFields()) {
            entrySource.addField((EntryField)entryField.clone());
        }

        entrySource.add = add;
        entrySource.bind = bind;
        entrySource.delete = delete;
        entrySource.modify = modify;
        entrySource.modrdn = modrdn;
        entrySource.search = search;

        return entrySource;
    }

    public boolean isPrimarySourceRef() {
        return primarySourceRef;
    }

    public void setPrimarySourceRef(boolean primarySourceRef) {
        this.primarySourceRef = primarySourceRef;
    }

    public void add(Session session, AddRequest request, AddResponse response) throws Exception {
        source.add(session, request, response);
    }

    public void bind(Session session, BindRequest request, BindResponse response) throws Exception {
        source.bind(session, request, response);
    }

    public void bind(Session session, BindRequest request, BindResponse response, Attributes attributes) throws Exception {
        source.bind(session, request, response, attributes);
    }

    public void compare(Session session, CompareRequest request, CompareResponse response) throws Exception {
        source.compare(session, request, response);
    }

    public void delete(Session session, DeleteRequest request, DeleteResponse response) throws Exception {
        source.delete(session, request, response);
    }

    public SearchResult find(Session session, String dn) throws Exception {
        return find(session, new DN(dn));
    }
    
    public SearchResult find(Session session, DN dn) throws Exception {

        SearchResponse response = search(session, dn, null, SearchRequest.SCOPE_BASE);

        if (response.getReturnCode() != LDAP.SUCCESS) {
            if (debug) log.debug("Entry "+dn+" not found: "+response.getErrorMessage());
            throw LDAP.createException(response.getReturnCode());
        }

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        return response.next();
    }

    public void modify(Session session, ModifyRequest request, ModifyResponse response) throws Exception {
        source.modify(session, request, response);
    }

    public void modrdn(Session session, ModRdnRequest request, ModRdnResponse response) throws Exception {
        source.modrdn(session, request, response);
    }

    public SearchResponse search(Session session, String filter) throws Exception {
        return search(session, null, filter, SearchRequest.SCOPE_SUB);
    }

    public SearchResponse search(Session session, Filter filter) throws Exception {
        return search(session, null, filter, SearchRequest.SCOPE_SUB);
    }

    public SearchResponse search(Session session, String baseDn, String filter, int scope) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        search(session, request, response);

        return response;
    }

    public SearchResponse search(Session session, DN baseDn, Filter filter, int scope) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(baseDn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        search(session, request, response);

        return response;
    }

    public void search(Session session, SearchRequest request, SearchResponse response) throws Exception {
        source.search(session, request, response);
    }

    public void unbind(Session session, UnbindRequest request, UnbindResponse response) throws Exception {
        source.unbind(session, request, response);
    }

    public String getMappingName() {
        return sourceConfig.getMappingName();
    }

    public Mapping getMapping() {
        return entry.getMapping(sourceConfig.getMappingName());
    }
}
