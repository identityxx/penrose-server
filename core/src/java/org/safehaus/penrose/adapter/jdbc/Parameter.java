package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.source.Field;

/**
 * @author Endi S. Dewata
 */
public class Parameter {

    protected Field field;
    protected Object value;

    public Parameter(Field field, Object value) {
        this.field = field;
        this.value = value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }
}
