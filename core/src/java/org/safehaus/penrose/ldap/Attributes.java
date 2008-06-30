package org.safehaus.penrose.ldap;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Attributes implements Serializable, Cloneable {

    public final static Collection<Object> EMPTY = new ArrayList<Object>();

    protected Map<String,Attribute> attributes = new TreeMap<String,Attribute>();

    public Attributes() {
    }

    public Collection<String> getNormalizedNames() {
        return attributes.keySet();
    }
    
    public Collection<String> getNames() {
        Collection<String> names = new TreeSet<String>();
        for (Attribute attribute : attributes.values()) {
            names.add(attribute.getName());
        }
        return names;
    }

    public void setValue(String name, Object value) {
/*
        if (value == null) {
            remove(name);
            return;
        }
*/
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValue(value);
    }

    public void addValue(String name, Object value) {
        //if (value == null) return;
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.addValue(value);
    }

    public void removeValue(String name, Object value) {
        if (value == null) return;
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) return;

        attribute.removeValue(value);
        if (!attribute.isEmpty()) return;

        attributes.remove(name.toLowerCase());
    }

    public void setValues(String name, Collection<Object> values) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValues(values);
    }

    public void addValues(String name, Collection<Object> values) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.addValues(values);
    }

    public void removeValues(String name, Collection<Object> values) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) return;

        attribute.removeValues(values);
        if (!attribute.isEmpty()) return;

        attributes.remove(name.toLowerCase());
    }

    public void set(Attributes attributes) {
        if (this == attributes) return;
        clear();
        add(attributes);
    }

    public void add(Attributes attributes) {
        for (Attribute attribute : attributes.getAll()) {
            add(attribute);
        }
    }
    
    public void add(Attribute attribute) {
        String name = attribute.getName();
        String normalizedName = name.toLowerCase();

        Attribute attr = attributes.get(normalizedName);
        if (attr == null) {
            attr = new Attribute(name);
            attributes.put(normalizedName, attr);
        }
        attr.addValues(attribute.getValues());
    }

    public void set(Attribute attribute) {
        String name = attribute.getName();
        String normalizedName = name.toLowerCase();

        attributes.put(normalizedName, attribute);
    }

    public Attribute get(String name) {
        return attributes.get(name.toLowerCase());
    }

    public Object getValue(String name) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) return null;
        return attribute.getValue();
    }

    public Collection<Object> getValues(String name) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) return EMPTY;
        return attribute.getValues();
    }

    public Attribute remove(String name) {
        return attributes.remove(name.toLowerCase());
    }

    public void remove(Collection<String> list) throws Exception {
        for (String attributeName : list) {
            remove(attributeName);
        }
    }

    public void retain(Collection<String> list) throws Exception {
        Collection<String> list2 = new ArrayList<String>();
        for (String attributeName : list) {
            list2.add(attributeName.toLowerCase());
        }

        Collection<String> normalizedNames = new ArrayList<String>();
        normalizedNames.addAll(attributes.keySet());

        for (String attributeName : normalizedNames) {
            if (list2.contains(attributeName)) continue;
            remove(attributeName);
        }
    }

    public Collection<Attribute> getAll() {
        return attributes.values();
    }
    
    public void clear() {
        attributes.clear();
    }

    public boolean isEmpty() {
        return attributes.isEmpty();
    }

    public Object clone() throws CloneNotSupportedException {
        Attributes object = (Attributes)super.clone();

        object.attributes = new LinkedHashMap<String,Attribute>();
        for (String name : attributes.keySet()) {
            Attribute attribute = attributes.get(name);
            object.attributes.put(name, (Attribute)attribute.clone());
        }

        return object;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (Attribute attribute : attributes.values()) {
            sb.append(attribute.toString());
        }

        return sb.toString();
    }

    public void print() throws Exception {
        for (Attribute attribute : attributes.values()) {
            attribute.print();
        }
    }
}
