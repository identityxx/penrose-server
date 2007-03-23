package org.safehaus.penrose.entry;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * @author Endi S. Dewata
 */
public class Attribute {

    protected String name;
    protected Collection values = new LinkedHashSet();

    public Attribute(String name) {
        this.name = name;
    }

    public Attribute(String name, Object value) {
        this.name = name;
        this.values.add(value);
    }

    public Attribute(String name, Collection values) {
        this.name = name;
        this.values.addAll(values);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        if (values.isEmpty()) return null;
        return values.iterator().next();
    }
    
    public Collection getValues() {
        return values;
    }

    public void setValue(Object value) {
        values.clear();
        values.add(value);
    }

    public void addValue(Object value) {
        values.add(value);
    }

    public void removeValue(Object value) {
        values.remove(value);
    }

    public void addValues(Collection values) {
        if (values != null) this.values.addAll(values);
    }

    public void setValues(Collection values) {
        if (this.values == values) return;
        this.values.clear();
        if (values != null) this.values.addAll(values);
    }

    public void removeValues(Collection values) {
        if (values != null) this.values.removeAll(values);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }
}
