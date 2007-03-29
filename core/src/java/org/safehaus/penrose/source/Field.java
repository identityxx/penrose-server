package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.FieldConfig;

/**
 * @author Endi S. Dewata
 */
public class Field {

    private FieldConfig fieldConfig;
    private String name;
    private String originalName;
    private boolean primaryKey;
    private boolean caseSensitive;
    private String type;

    public Field(FieldConfig fieldConfig) {
        this.fieldConfig = fieldConfig;

        name = fieldConfig.getName();
        originalName = fieldConfig.getOriginalName();
        primaryKey = fieldConfig.isPrimaryKey();
        caseSensitive = fieldConfig.isCaseSensitive();
        type = fieldConfig.getType();
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

    public void setName(String name) {
        this.name = name;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getOriginalName() {
        return originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }
}
