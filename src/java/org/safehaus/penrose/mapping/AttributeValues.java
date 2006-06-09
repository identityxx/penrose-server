package org.safehaus.penrose.mapping;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * This class holds entry's attribute values. Each attribute value is a collection.
 *
 * @author Endi S. Dewata
 */
public class AttributeValues implements Cloneable, Comparable {

    Logger log = Logger.getLogger(getClass());

    public Map values = new TreeMap();

    public AttributeValues() {
    }

    public AttributeValues(AttributeValues attributeValues) {
        add(attributeValues);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }
    
    public void add(AttributeValues attributeValues) {
        add(null, attributeValues);
    }

    public void add(String prefix, AttributeValues attributeValues) {
        if (attributeValues == null) return;
        Map v = attributeValues.getValues();
        for (Iterator i = v.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection c = (Collection)v.get(name);
            add(prefix == null ? name : prefix+"."+name, c);
        }
    }

    public void add(Row row) {
        add(null, row);
    }

    public void add(String prefix, Row row) {
        for (Iterator i = row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            Collection c = get(name);
            if (c == null) c = new HashSet();
            c.add(value);
            set(prefix == null ? name : prefix+"."+name, c);
        }
    }

    public void set(Row row) {
        set(null, row);
    }

    public void set(String prefix, Row row) {
        for (Iterator i = row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            Collection c = new HashSet();
            c.add(value);
            set(prefix == null ? name : prefix+"."+name, c);
        }
    }

    public void set(String prefix, AttributeValues attributeValues) {
        if (attributeValues == null) {
            remove(prefix);
            return;
        }

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection c = attributeValues.get(name);
            set(prefix+"."+name, c);
        }
    }

    public void set(String name, Collection values) {
        Collection c = new HashSet();
        c.addAll(values);
        this.values.put(name, c);
    }

    public void set(String name, Object value) {

        if (value == null) return;

        if (value instanceof Collection) {
            set(name, (Collection)value);
            return;
        }

        Collection c = new HashSet();
        c.add(value);
        this.values.put(name, c);
    }

    public void add(String name, Object value) {

        if (value == null) return;

        if (value instanceof Collection) {
            add(name, (Collection)value);
            return;
        }
        
        Collection c = (Collection)this.values.get(name);
        if (c == null) {
            //c = new TreeSet();
            c = new HashSet();
            this.values.put(name, c);
        }
        c.add(value);
    }

    public void add(String name, Object objects[]) {
        add(name, Arrays.asList(objects));
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

    public boolean contains(Row row) {
        return contains(null, row);
    }
    
    public boolean contains(String prefix, Row row) {
        
        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String fullName = (prefix == null ? name : prefix+"."+name);

            Object value = row.get(name);
            Collection list = get(fullName);

            if (value == null || list == null) return false;
            if (!list.contains(value)) return false;
        }

        return true;
    }

    public void clear() {
        values.clear();
    }

    public void remove(String name) {
        Collection list = new ArrayList();
        list.addAll(values.keySet());

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String s = (String)i.next();
            if (s.equals(name) || s.startsWith(name+".")) {
                values.remove(s);
            }
        }
    }

    public void remove(String name, Object value) {
        if (value == null) return;

        Collection c = (Collection)values.get(name);
        if (c == null) return;

        c.remove(value);
        if (c.isEmpty()) values.remove(name);
    }

    public void remove(String name, Collection values) {
        if (values == null) return;

        Collection c = (Collection)this.values.get(name);
        if (c == null) return;

        c.removeAll(values);
        if (c.isEmpty()) this.values.remove(name);
    }

    public void retain(Collection names) {
        Collection lcNames = new ArrayList();
        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            lcNames.add(name.toLowerCase());
        }

        Collection list = new ArrayList();
        list.addAll(values.keySet());

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String s = (String)i.next();

            int p = s.indexOf(".");
            String n = p >= 0 ? s.substring(0, p) : s;

            if (!lcNames.contains(n.toLowerCase())) values.remove(s);
        }
    }

    public void retain(String name) {
        Collection list = new ArrayList();
        list.addAll(values.keySet());

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String s = (String)i.next();

            int p = s.indexOf(".");
            String n = p >= 0 ? s.substring(0, p) : s;

            if (!name.equalsIgnoreCase(n)) values.remove(s);
        }
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

    public int compareTo(Object object) {

        int c = 0;

        try {
            if (object == null) return 0;
            if (!(object instanceof AttributeValues)) return 0;

            AttributeValues attributeValues = (AttributeValues)object;

            Iterator i = values.keySet().iterator();
            Iterator j = attributeValues.values.keySet().iterator();

            while (i.hasNext() && j.hasNext()) {
                String name1 = (String)i.next();
                String name2 = (String)j.next();

                c = name1.compareTo(name2);
                if (c != 0) return c;

                Collection values1 = (Collection)values.get(name1);
                Collection values2 = (Collection)attributeValues.values.get(name2);

                Iterator k = values1.iterator();
                Iterator l = values2.iterator();

                while (k.hasNext() && l.hasNext()) {
                    Object value1 = k.next();
                    Object value2 = l.next();

                    if (value1 instanceof Comparable && value2 instanceof Comparable) {
                        Comparable v1 = (Comparable)value1.toString();
                        Comparable v2 = (Comparable)value2.toString();

                        c = v1.compareTo(v2);
                        if (c != 0) return c;
                    }
                }

                if (k.hasNext()) return 1;
                if (l.hasNext()) return -1;
            }

            if (i.hasNext()) return 1;
            if (j.hasNext()) return -1;

        } finally {
            //System.out.println("Comparing "+this+" with "+object+": "+c);
        }

        return c;
    }
}
