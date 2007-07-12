package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.Expression;

/**
 * @author Endi S. Dewata
 */
public class Field {

    private FieldConfig fieldConfig;
    private String name;
    private String originalName;
    private boolean primaryKey;
    private boolean caseSensitive;

    private int length;
    private int defaultLength;
    private String type;

    public Field(FieldConfig fieldConfig) {
        this.fieldConfig = fieldConfig;

        name = fieldConfig.getName();
        originalName = fieldConfig.getOriginalName();
        primaryKey = fieldConfig.isPrimaryKey();
        caseSensitive = fieldConfig.isCaseSensitive();

        length = fieldConfig.getLength();
        defaultLength = fieldConfig.getDefaultLength();
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

    public int getLength() {
        return length;
    }

    public int getDefaultLength() {
        return defaultLength;
    }

    public void setDefaultLength(int defaultLength) {
        this.defaultLength = defaultLength;
    }
    
    public void setLength(int length) {
        this.length = length;
    }

    public Object getConstant() {
        return fieldConfig.getConstant();
    }
    
    public String getVariable() {
        return fieldConfig.getVariable();
    }

    public Expression getExpression() {
        return fieldConfig.getExpression();
    }
}
