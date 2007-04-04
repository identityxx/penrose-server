package org.safehaus.penrose.entry;

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

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
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);
            values.put(prefix == null ? name : prefix+"."+name, value);
        }
    }

    public void set(String prefix, RDN rdn) {
        values.clear();
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);
            values.put(prefix == null ? name : prefix+"."+name, value);
        }
    }

    public void set(String name, Object value) {
        this.values.put(name, value);
    }

    public Object remove(String name) {
        return values.remove(name);
    }

    public void normalize() {
        for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = values.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }

            values.put(name, value);
        }
    }

    public RDN toRdn() {
        return new RDN(values);
    }
}
