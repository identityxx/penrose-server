package org.safehaus.penrose.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.BinaryUtil;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Attributes implements Cloneable {

    public final static Collection<Object> EMPTY = new ArrayList<Object>();

    protected Collection<String> names = new TreeSet<String>();
    protected Map<String,Attribute> attributes = new TreeMap<String,Attribute>();

    public Attributes() {
    }

    public Collection<String> getNames() {
        return names;
    }

    public void setValue(String name, Object value) {

        if (value == null) {
            remove(name);
            return;
        }

        names.add(name);
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValue(value);
    }

    public void addValue(String name, Object value) {
        if (value == null) return;
        names.add(name);
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
        names.remove(name);
    }

    public void setValues(String name, Collection<Object> values) {
        names.add(name);
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValues(values);
    }

    public void addValues(String name, Collection<Object> values) {
        names.add(name);
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
        names.remove(name);
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

        names.add(name);

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

        names.add(name);
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
        names.remove(name);
        return attributes.remove(name.toLowerCase());
    }

    public Collection<Attribute> getAll() {
        return attributes.values();
    }
    
    public void clear() {
        names.clear();
        attributes.clear();
    }

    public boolean isEmpty() {
        return names.isEmpty();
    }

    public Object clone() throws CloneNotSupportedException {
        Attributes object = (Attributes)super.clone();

        object.names = new LinkedHashSet<String>();
        object.names.addAll(names);

        object.attributes = new LinkedHashMap<String,Attribute>();
        for (String normalizedName : attributes.keySet()) {
            Attribute attribute = attributes.get(normalizedName);
            object.attributes.put(normalizedName, (Attribute)attribute.clone());
        }

        return object;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (Attribute attribute : attributes.values()) {
            sb.append(attribute);
        }

        return sb.toString();
    }

    public void print() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());

        for (Attribute attribute : attributes.values()) {

            String name = attribute.getName();
            Collection list = attribute.getValues();

            for (Object value : list) {
                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".") + 1);

                if (value instanceof byte[]) {
                    value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[]) value);
                }

                log.debug(" - " + name + ": " + value + " (" + className + ")");
            }
        }
    }
}
