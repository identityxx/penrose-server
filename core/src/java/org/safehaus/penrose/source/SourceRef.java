package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceRef {

    private Source source;

    private String alias;
    private boolean required;

    Collection<FieldRef> primaryKeyFieldRefs = new ArrayList<FieldRef>();
    Map<String,FieldRef> fieldRefs = new LinkedHashMap<String,FieldRef>();

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public SourceRef(Source source) {
        this.source = source;

        this.alias = source.getName();
        this.required = true;

        for (Field field : source.getFields()) {
            String fieldName = field.getName();

            FieldRef fieldRef = new FieldRef(field, alias, null);
            fieldRefs.put(fieldName, fieldRef);

            if (field.isPrimaryKey()) primaryKeyFieldRefs.add(fieldRef);
        }
    }

    public SourceRef(Source source, SourceMapping sourceMapping) throws Exception {
        this.source = source;

        this.alias = sourceMapping.getName();
        this.required = sourceMapping.isRequired();
        this.parameters.putAll(sourceMapping.getParameters());

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            String fieldName = fieldMapping.getName();

            Field field = source.getField(fieldName);
            if (field == null) throw new Exception("Unknown field: " + fieldName);

            FieldRef fieldRef = new FieldRef(field, alias, fieldMapping);
            fieldRefs.put(fieldName, fieldRef);

            if (field.isPrimaryKey()) primaryKeyFieldRefs.add(fieldRef);
        }
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

    public boolean isRequired() {
        return required;
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

    public void setRequired(boolean required) {
        this.required = required;
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
}
