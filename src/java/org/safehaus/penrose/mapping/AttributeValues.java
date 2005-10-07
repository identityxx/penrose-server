package org.safehaus.penrose.mapping;

import java.util.*;

/**
 * This class holds entry's attribute values. Each attribute value is a collection.
 *
 * @author Endi S. Dewata
 */
public class AttributeValues implements Cloneable {

    public Map values = new TreeMap();

    public AttributeValues() {
    }

    public AttributeValues(AttributeValues attributeValues) {
        add(attributeValues);
    }

    public void add(AttributeValues attributeValues) {
        add(null, attributeValues);
    }

    public void add(String prefix, AttributeValues attributeValues) {
        Map v = attributeValues.getValues();
        for (Iterator i = v.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection c = (Collection)v.get(name);
            add(prefix == null ? name : prefix+"."+name, c);
        }
    }

    public void add(Row row) {
        for (Iterator i = row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            Collection c = get(name);
            if (c == null) {
                //c = new TreeSet();
                c = new HashSet();
                set(name, c);
            }
            c.add(value);
        }
    }

    public void set(Row row) {
        for (Iterator i = row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            Collection c = new HashSet();
            c.add(value);
            set(name, c);
        }
    }

    public void add(String name, Object value) {
        if (value instanceof Collection) {
            add(name, (Collection)value);
            return;
        }
        
        if (value == null) return;

        Collection c = (Collection)this.values.get(name);
        if (c == null) {
            //c = new TreeSet();
            c = new HashSet();
            this.values.put(name, c);
        }
        c.add(value);
    }

    public void add(String name, Collection values) {
        if (values == null) return;
        Collection c = (Collection)this.values.get(name);
        if (c == null) {
            //c = new TreeSet();
            c = new HashSet();
            this.values.put(name, c);
        }
        c.addAll(values);
    }

    public void set(String name, Collection values) {
        Collection c = new HashSet();
        c.addAll(values);
        this.values.put(name, c);
    }

    public Collection get(String name) {
        return (Collection)values.get(name);
    }

    public Object getOne(String name) {
        Collection c = (Collection)values.get(name);
        if (c == null || c.isEmpty()) return null;
        return c.iterator().next();
    }
    
    public Collection getNames() {
        return values.keySet();
    }

    public boolean contains(String name) {
        boolean b = values.containsKey(name);
        if (b) return true;

        for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
            String s = (String)i.next();
            if (s.startsWith(name+".")) return true;
        }

        return false;
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

    public Object clone() {
        AttributeValues attributeValues = new AttributeValues();
        for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection c = (Collection)values.get(name);
            Collection s = new HashSet();
            s.addAll(c);
            attributeValues.values.put(name, s);
        }
        return attributeValues;
    }
}
