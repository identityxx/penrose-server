package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.Expression;

/**
 * @author Endi S. Dewata
 */
public class Field implements Cloneable {

    private Source source;
    private FieldConfig fieldConfig;

    public Field(Source source, FieldConfig fieldConfig) {
        this.source = source;
        this.fieldConfig = fieldConfig;
    }
    
    public FieldConfig getFieldConfig() {
        return fieldConfig;
    }

    public void setFieldConfig(FieldConfig fieldConfig) {
        this.fieldConfig = fieldConfig;
    }

    public String getName() {
        return fieldConfig.getName();
    }

    public boolean isPrimaryKey() {
        return fieldConfig.isPrimaryKey();
    }

    public boolean isIndex() {
        return fieldConfig.isIndex();
    }

    public String getOriginalName() {
        return fieldConfig.getOriginalName();
    }

    public String getType() {
        return fieldConfig.getType();
    }

    public String getCastType() {
        return fieldConfig.getCastType();
    }

    public boolean isCaseSensitive() {
        return fieldConfig.isCaseSensitive();
    }

    public int getLength() {
        return fieldConfig.getLength();
    }

    public int getDefaultLength() {
        return fieldConfig.getDefaultLength();
    }

    public Object getConstant() {
        return fieldConfig.getConstant();
    }
    
    public String getVariable() {
        return fieldConfig.getVariable();
    }

    public void setVariable(String variable) {
        fieldConfig.setVariable(variable);
    }

    public Expression getExpression() {
        return fieldConfig.getExpression();
    }

    public Object clone() throws CloneNotSupportedException {
        Field field = (Field)super.clone();
        field.fieldConfig = (FieldConfig)fieldConfig.clone();
        return field;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public boolean isText() {
        return fieldConfig.isText();
    }

    public String toString() {
        return source.getName()+"."+fieldConfig.getName();
    }
}
