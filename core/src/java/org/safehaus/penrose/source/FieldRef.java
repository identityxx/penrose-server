package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.FieldMapping;

import java.util.Collection;
import java.util.HashSet;

/**
 * @author Endi S. Dewata
 */
public class FieldRef {

    private Field field;
    private FieldConfig fieldConfig;

    private String sourceName;

    private String name;
    private String originalName;
    private String type;
    private boolean caseSensitive;
    private boolean primaryKey;

    private Collection<String> operations = new HashSet<String>();

    private FieldMapping fieldMapping;

    public FieldRef(Field field, String sourceName, FieldMapping fieldMapping) {
        this(field.getFieldConfig(), sourceName, fieldMapping);
        this.field = field;
    }

    public FieldRef(FieldConfig fieldConfig, String sourceName, FieldMapping fieldMapping) {
        this.fieldConfig = fieldConfig;

        originalName = fieldConfig.getOriginalName();
        type = fieldConfig.getType();
        caseSensitive = fieldConfig.isCaseSensitive();
        primaryKey = fieldConfig.isPrimaryKey();

        this.sourceName = sourceName;
        this.fieldMapping = fieldMapping;

        if (fieldMapping == null) {
            name = fieldConfig.getName();
        } else {
            name = fieldMapping.getName();
            operations.addAll(fieldMapping.getOperations());
        }
    }
    
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public FieldMapping getFieldMapping() {
        return fieldMapping;
    }

    public void setFieldMapping(FieldMapping fieldMapping) {
        this.fieldMapping = fieldMapping;
    }

    public FieldConfig getFieldConfig() {
        return fieldConfig;
    }

    public void setFieldConfig(FieldConfig fieldConfig) {
        this.fieldConfig = fieldConfig;
    }

    public String getName() {
        return name;
    }

    public String getOriginalName() {
        return originalName;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Collection<String> getOperations() {
        return operations;
    }

    public void setOperations(Collection<String> operations) {
        this.operations = operations;
    }
}
