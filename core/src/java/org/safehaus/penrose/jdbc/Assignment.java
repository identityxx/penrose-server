package org.safehaus.penrose.jdbc;

/**
 * @author Endi S. Dewata
 */
public class Assignment {

    protected String name;
    protected Object value;

    public Assignment(Object value) {
        this.value = value;
    }
    
    public Assignment(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
