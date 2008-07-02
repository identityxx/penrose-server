package org.safehaus.penrose.directory;

import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceRef implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private Source source;

    private String alias;
    private boolean primarySourceRef;

    Map<String,Collection<FieldRef>> fieldRefs = new LinkedHashMap<String,Collection<FieldRef>>();
    Map<String,FieldRef> primaryKeyFieldRefs = new LinkedHashMap<String,FieldRef>();

    private String add;
    private String bind;
    private String delete;
    private String modify;
    private String modrdn;
    private String search;

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public SourceRef(Source source) throws Exception {
        this.source = source;
        this.alias = source.getName();

        //if (debug) log.debug("Source ref "+source.getName()+" "+alias+":");

        for (Field field : source.getFields()) {
            //if (debug) log.debug(" - field "+field.getName());

            FieldRef fieldRef = new FieldRef(this, field);
            addFieldRef(fieldRef);
        }
    }

    public SourceRef(Entry entry, Source source, SourceMapping sourceMapping) throws Exception {
        this.source = source;

        this.alias = sourceMapping.getName();

        String primarySourceName = entry.getPrimarySourceName();
        this.primarySourceRef = alias.equals(primarySourceName);

        if (debug) log.debug("Source ref "+source.getName()+" "+alias+":");

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            String fieldName = fieldMapping.getName();
            if (debug) log.debug(" - field "+fieldName);

            Field field = source.getField(fieldName);
            if (field == null) throw new Exception("Unknown field: " + fieldName);

            FieldRef fieldRef = new FieldRef(entry, this, field, fieldMapping);
            addFieldRef(fieldRef);
        }

        add = sourceMapping.getAdd();
        bind = sourceMapping.getBind();
        delete = sourceMapping.getDelete();
        modify = sourceMapping.getModify();
        modrdn = sourceMapping.getModrdn();
        search = sourceMapping.getSearch();

        this.parameters.putAll(sourceMapping.getParameters());
    }

    public void addFieldRef(FieldRef fieldRef) {
        String fieldName = fieldRef.getName();
        Collection<FieldRef> list = fieldRefs.get(fieldName);
        if (list == null) {
            list = new ArrayList<FieldRef>();
            fieldRefs.put(fieldName, list);
        }
        list.add(fieldRef);

        if (fieldRef.isPrimaryKey()) primaryKeyFieldRefs.put(fieldName, fieldRef);
    }

    public Collection<FieldRef> getPrimaryKeyFieldRefs() {
        return primaryKeyFieldRefs.values();
    }

    public FieldRef getPrimaryKeyFieldRef(String fieldName) {
        return primaryKeyFieldRefs.get(fieldName);
    }

    public Collection<FieldRef> getFieldRefs() {
        Collection<FieldRef> results = new ArrayList<FieldRef>();
        for (Collection<FieldRef> list : fieldRefs.values()) {
            results.addAll(list);
        }
        return results;
    }

    public FieldRef getFieldRef(String fieldName) {
        Collection<FieldRef> results = getFieldRefs(fieldName);
        if (results.isEmpty()) return null;
        return results.iterator().next();
    }

    public Collection<FieldRef> getFieldRefs(String fieldName) {
        Collection<FieldRef> results = new ArrayList<FieldRef>();
        Collection<FieldRef> list = fieldRefs.get(fieldName);
        if (list == null || list.isEmpty()) return results;
        results.addAll(list);
        return results;
    }

    public String getAlias() {
        return alias;
    }

    public String getSearch() {
        return search;
    }

    public String toString() {
        return alias;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;

        for (FieldRef fieldRef : getFieldRefs()) {
            Field field = fieldRef.getField();
            fieldRef.setField(field);
        }
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
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

    public Object clone() throws CloneNotSupportedException {

        SourceRef sourceRef = (SourceRef)super.clone();

        sourceRef.source = source;

        sourceRef.alias = alias;
        sourceRef.primarySourceRef = primarySourceRef;

        sourceRef.fieldRefs = new LinkedHashMap<String,Collection<FieldRef>>();
        sourceRef.primaryKeyFieldRefs = new LinkedHashMap<String,FieldRef>();

        for (FieldRef fieldRef : getFieldRefs()) {
            sourceRef.addFieldRef((FieldRef)fieldRef.clone());
        }

        sourceRef.add = add;
        sourceRef.bind = bind;
        sourceRef.delete = delete;
        sourceRef.modify = modify;
        sourceRef.modrdn = modrdn;
        sourceRef.search = search;

        sourceRef.parameters = new LinkedHashMap<String,String>();
        sourceRef.parameters.putAll(parameters);

        return sourceRef;
    }

    public boolean isPrimarySourceRef() {
        return primarySourceRef;
    }

    public void setPrimarySourceRef(boolean primarySourceRef) {
        this.primarySourceRef = primarySourceRef;
    }
}
