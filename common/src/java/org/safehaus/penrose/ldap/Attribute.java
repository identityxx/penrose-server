package org.safehaus.penrose.ldap;

import org.safehaus.penrose.util.BinaryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Arrays;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Attribute implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    protected String name;
    protected Collection<Object> values = new LinkedHashSet<Object>();

    public Attribute(String name) {
        this.name = name;
    }

    public Attribute(String name, Object value) {
        this.name = name;
        this.values.add(value);
    }

    public Attribute(String name, Collection<Object> values) {
        this.name = name;
        if (values == null) return;
        for (Object value : values) {
            if (!containsValue(value)) this.values.add(value);
        }
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
    
    public Collection<Object> getValues() {
        return values;
    }

    public void setValue(Object value) {
        values.clear();
        if (value == null) return;
        values.add(value);
    }

    public void addValue(Object value) {
        if (value == null || containsValue(value)) return;
        values.add(value);
    }

    public boolean containsValue(Object value) {
        return getValue(value) != null;
    }

    public Object getValue(Object value) {
        //Logger log = LoggerFactory.getLoggerConfig(getClass());

        if (value == null) return null;

        //log.debug("Searching for ["+value+"] "+value.getClass().getName());

        if (values.contains(value)) {
            //log.debug("Found ["+value+"] "+value.getClass().getName());
            return value;
        }

        for (Object v : values) {

            //log.debug("Comparing with ["+v+"] "+v.getClass().getName());

            if (v instanceof byte[]) {
                byte[] b = (byte[])v;

                if (value instanceof byte[]) {
                    byte[] bytes = (byte[])value;
                    if (Arrays.equals(b, bytes)) {
                        //log.debug("Found ["+v+"] "+v.getClass().getName());
                        return v;
                    }

                } else if (value instanceof String) {
                    byte[] bytes = ((String)value).getBytes();
                    if (Arrays.equals(b, bytes)) {
                        //log.debug("Found ["+v+"] "+v.getClass().getName());
                        return v;
                    }
                }

            } else if (v instanceof String) {
                String s = (String)v;

                if (value instanceof String) {
                    String string = (String)value;
                    if (s.equalsIgnoreCase(string)) {
                        //log.debug("Found ["+v+"] "+v.getClass().getName());
                        return v;
                    }

                } else if (value instanceof byte[]) {
                    byte[] b = ((String)v).getBytes();
                    byte[] bytes = (byte[])value;
                    if (Arrays.equals(b, bytes)) {
                        //log.debug("Found ["+v+"] "+v.getClass().getName());
                        return v;
                    }
                }

            } else if (v.equals(value)) {
                //log.debug("Found ["+v+"] "+v.getClass().getName());
                return v;
            }
        }

        //log.debug("Not found ["+value+"] "+value.getClass().getName());
        
        return null;
    }

    public void removeValue(Object value) {
        if (value == null) return;

        Object v = getValue(value);
        if (v == null) return;

        values.remove(v);
    }

    public void addValues(Collection<Object> values) {
        if (values == null) return;
        for (Object value : values) {
            Object v = getValue(value);
            if (v == null) this.values.add(value);
        }
    }

    public void setValues(Collection<Object> values) {
        if (this.values == values) return;
        this.values.clear();
        if (values == null) return;
        for (Object value : values) {
            Object v = getValue(value);
            if (v == null) this.values.add(value);
        }
    }

    public void removeValues(Collection<Object> values) {
        if (values == null) return;
        for (Object value : values) {
            Object v = getValue(value);
            if (v != null) {
                this.values.remove(v);
            }
        }
    }

    public void clear() {
        values.clear();
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public int getSize() {
        return values.size();
    }

    public Object clone() throws CloneNotSupportedException {
        Attribute attribute = (Attribute)super.clone();

        attribute.name = name;

        attribute.values = new LinkedHashSet<Object>();
        attribute.values.addAll(values);

        return attribute;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();

        for (Object value : values) {
            sb.append(name);
            sb.append(":");

            if (value instanceof byte[]) {
                sb.append(":");
                value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[]) value);
            }

            sb.append(" ");
            sb.append(value);
            sb.append("\n");
        }

        return sb.toString();
    }

    public void print() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        for (Object value : values) {
            String className = value.getClass().getName();
            className = className.substring(className.lastIndexOf(".") + 1);

            if (value instanceof byte[]) {
                value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[]) value, 0, 10)+"...";
            }

            if (debug) log.debug(" - " + name + ": " + value + " (" + className + ")");
        }

        if (isEmpty()) {
            if (debug) log.debug(" - " + name);
        }
    }
}
