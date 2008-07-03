package org.safehaus.penrose.ldap;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class RDNBuilder {

    public Map<String,Object> values = new TreeMap<String,Object>();

    public RDNBuilder() {
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public void clear() {
        values.clear();
    }

    public void add(RDN rdn) {
        values.putAll(rdn.getValues());
    }

    public void set(RDN rdn) {
        values.clear();
        values.putAll(rdn.getValues());
    }

    public void add(String prefix, RDN rdn) {
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);
            values.put(prefix == null ? name : prefix + "." + name, value);
        }
    }

    public void set(String prefix, RDN rdn) {
        values.clear();
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);
            values.put(prefix == null ? name : prefix + "." + name, value);
        }
    }

    public void set(String name, Object value) {
        this.values.put(name, value);
    }

    public Object remove(String name) {
        return values.remove(name);
    }

    public void normalize() {
        for (String name : values.keySet()) {
            Object value = values.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String) value).toLowerCase();
            }

            values.put(name, value);
        }
    }

    public RDN toRdn() {
        return new RDN(values);
    }
}
