package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceRef {

    public Logger log = LoggerFactory.getLogger(getClass());

    private Source source;

    private String alias;

    Collection<FieldRef> primaryKeyFieldRefs = new ArrayList<FieldRef>();
    Map<String,FieldRef> fieldRefs = new LinkedHashMap<String,FieldRef>();

    private String add;
    private String bind;
    private String delete;
    private String modify;
    private String modrdn;
    private String search;

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public SourceRef(Source source) {
        this.source = source;

        this.alias = source.getName();

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Source ref "+source.getName()+" "+alias+":");

        for (Field field : source.getFields()) {
            String fieldName = field.getName();
            if (debug) log.debug(" - field "+fieldName);

            FieldRef fieldRef = new FieldRef(field, alias, null);
            fieldRefs.put(fieldName, fieldRef);

            if (field.isPrimaryKey()) primaryKeyFieldRefs.add(fieldRef);
        }
    }

    public SourceRef(Source source, SourceMapping sourceMapping) throws Exception {
        this.source = source;

        this.alias = sourceMapping.getName();

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Source ref "+source.getName()+" "+alias+":");

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            String fieldName = fieldMapping.getName();
            if (debug) log.debug(" - field "+fieldName);

            Field field = source.getField(fieldName);
            if (field == null) throw new Exception("Unknown field: " + fieldName);

            FieldRef fieldRef = new FieldRef(field, alias, fieldMapping);
            fieldRefs.put(fieldName, fieldRef);

            if (field.isPrimaryKey()) primaryKeyFieldRefs.add(fieldRef);
        }

        add = sourceMapping.getAdd();
        bind = sourceMapping.getBind();
        delete = sourceMapping.getDelete();
        modify = sourceMapping.getModify();
        modrdn = sourceMapping.getModrdn();
        search = sourceMapping.getSearch();

        this.parameters.putAll(sourceMapping.getParameters());
    }

    public Collection<FieldRef> getPrimaryKeyFieldRefs() {
        return primaryKeyFieldRefs;
    }

    public Collection<FieldRef> getFieldRefs() {
        return fieldRefs.values();
    }

    public FieldRef getFieldRef(String fieldName) {
        return fieldRefs.get(fieldName);
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
}
