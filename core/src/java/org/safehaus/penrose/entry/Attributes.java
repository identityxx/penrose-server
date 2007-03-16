package org.safehaus.penrose.entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.BinaryUtil;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Attributes {

    public final static Collection EMPTY = new ArrayList();

    protected Collection names = new LinkedHashSet();
    protected Map attributes = new LinkedHashMap();

    public Attributes() {
    }

    public Collection getNames() {
        return names;
    }

    public void setValue(String name, Object value) {
        names.add(name);
        Attribute attribute = (Attribute)attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValue(value);
    }

    public void addValue(String name, Object value) {
        names.add(name);
        Attribute attribute = (Attribute)attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.addValue(value);
    }

    public void setValues(String name, Collection values) {
        names.add(name);
        Attribute attribute = (Attribute)attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.setValues(values);
    }

    public void addValues(String name, Collection values) {
        names.add(name);
        Attribute attribute = (Attribute)attributes.get(name.toLowerCase());
        if (attribute == null) {
            attribute = new Attribute(name);
            attributes.put(name.toLowerCase(), attribute);
        }
        attribute.addValues(values);
    }

    public void add(Attribute attribute) {
        String name = attribute.getName();
        names.add(name);
        Attribute attr = (Attribute)attributes.get(name.toLowerCase());
        if (attr == null) {
            attributes.put(name.toLowerCase(), attribute);
        } else {
            attr.addValues(attribute.getValues());
        }
    }

    public Attribute get(String name) {
        return (Attribute)attributes.get(name.toLowerCase());
    }

    public Object getValue(String name) {
        Attribute attribute = (Attribute)attributes.get(name.toLowerCase());
        if (attribute == null) return null;
        return attribute.getValue();
    }

    public Collection getValues(String name) {
        Attribute attribute = (Attribute)attributes.get(name.toLowerCase());
        if (attribute == null) return EMPTY;
        return attribute.getValues();
    }

    public Attribute remove(String name) {
        names.remove(name);
        return (Attribute)attributes.remove(name.toLowerCase());
    }

    public Collection getAll() {
        return attributes.values();
    }
    
    public void clear() {
        names.clear();
        attributes.clear();
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
