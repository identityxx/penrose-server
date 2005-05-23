package org.safehaus.penrose.mapping;

import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;

/**
 * This class holds entry's attribute values. Each attribute value is a collection.
 *
 * @author Endi S. Dewata
 */
public class AttributeValues {

    public Map values = new TreeMap();

    public AttributeValues() {
    }

    public AttributeValues(AttributeValues attributes) {
        values.putAll(attributes.getValues());
    }

    public void set(String name, Collection values) {
        this.values.put(name, values);
    }

    public Collection get(String name) {
        return (Collection)values.get(name);
    }

    public Collection getNames() {
        return values.keySet();
    }

    public boolean contains(String name) {
        return values.containsKey(name);
    }

    public Map getValues() {
        return values;
    }

    public String toString() {
        return values.toString();
    }


    public int hashCode() {
        return values.hashCode();
    }

    public boolean equals(Object object) {
        if (object == null) return false;
        if (!(object instanceof AttributeValues)) return false;
        AttributeValues av = (AttributeValues)object;
        return values.equals(av.values);
    }
}
