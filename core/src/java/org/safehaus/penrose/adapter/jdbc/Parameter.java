package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.partition.FieldConfig;

/**
 * @author Endi S. Dewata
 */
public class Parameter {

    FieldConfig fieldConfig;
    Object value;

    public Parameter(FieldConfig fieldConfig, Object value) {
        this.fieldConfig = fieldConfig;
        this.value = value;
    }

    public void setFieldConfig(FieldConfig fieldConfig) {
        this.fieldConfig = fieldConfig;
    }

    public FieldConfig getFieldConfig() {
        return fieldConfig;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
