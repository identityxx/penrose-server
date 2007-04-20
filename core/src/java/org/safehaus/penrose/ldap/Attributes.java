package org.safehaus.penrose.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.BinaryUtil;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Attributes {

    public final static Collection EMPTY = new ArrayList();

    protected Collection<String> names = new LinkedHashSet<String>();
    protected Map<String,Attribute> attributes = new LinkedHashMap<String,Attribute>();

    public Attributes() {
    }

    public Collection<String> getNames() {
        return names;
    }

    public void setValue(String name, Object value) {
        names.add(name);
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValue(value);
    }

    public void addValue(String name, Object value) {
        names.add(name);
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.addValue(value);
    }

    public void removeValue(String name, Object value) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) return;

        attribute.removeValue(value);
        if (!attribute.isEmpty()) return;

        attributes.remove(name.toLowerCase());
        names.remove(name);
    }

    public void setValues(String name, Collection values) {
        names.add(name);
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValues(values);
    }

    public void addValues(String name, Collection values) {
        names.add(name);
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.addValues(values);
    }

    public void removeValues(String name, Collection values) {
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
        add(null, attributes);
    }

    public void add(String prefix, Attributes attributes) {
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            add(prefix, attribute);
        }
    }
    
    public void add(Attribute attribute) {
        add(null, attribute);
    }

    public void add(String prefix, Attribute attribute) {
        String name = prefix == null ? attribute.getName() : prefix+"."+attribute.getName();
        String normalizedName = name.toLowerCase();

        names.add(name);

        Attribute attr = attributes.get(normalizedName);
        if (attr == null) {
            attr = new Attribute(name);
            attributes.put(normalizedName, attr);
        }
        attr.addValues(attribute.getValues());
    }

    public Attribute get(String name) {
        return (Attribute)attributes.get(name.toLowerCase());
    }

    public Object getValue(String name) {
        Attribute attribute = attributes.get(name.toLowerCase());
        if (attribute == null) return null;
        return attribute.getValue();
    }

    public Collection getValues(String name) {
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

    public void print() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());

        for (Iterator i=attributes.values().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String name = attribute.getName();
            Collection list = attribute.getValues();

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Object value = j.next();
                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".")+1);

                if (value instanceof byte[]) {
                    value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])value);
                }

                log.debug(" - "+name+": "+value+" ("+className+")");
            }
        }
    }
}
