package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;

/**
 * @author Endi S. Dewata
 */
public class Assignment {

    protected Field field;
    protected Object value;

    public Assignment(FieldRef fieldRef, Object value) {
        this.field = fieldRef.getField();
        this.value = value;
    }

    public Assignment(Field field, Object value) {
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
