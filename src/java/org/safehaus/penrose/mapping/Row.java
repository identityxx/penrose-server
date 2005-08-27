/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;

/**
 * This class holds source's column values. Each value is an single object, not necessarily a collection.
 *
 * @author Endi S. Dewata
 */
public class Row implements Comparable {

    public Map values = new TreeMap();

    public Row() {
    }

    public Row(Map values) {
        this.values.putAll(values);
    }

    public Row(Row row) {
        values.putAll(row.getValues());
    }

    public void add(Row row) {
        values.putAll(row.getValues());
    }

    public void set(String name, Object value) {
        this.values.put(name, value);
    }

    public Object get(String name) {
        return values.get(name);
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
        StringBuffer sb = new StringBuffer();
        for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = values.get(name);

            if (sb.length() > 0) sb.append("+");
            sb.append(name);
            sb.append("=");
            sb.append(value);
            //sb.append(value+"("+value.getClass().getName()+")");
        }

        return sb.toString();
    }

    public int hashCode() {
        //System.out.println("Row "+values+" hash code: "+values.hashCode());
        return values.hashCode();
    }

    public boolean equals(Object object) {
        //System.out.println("Comparing row "+values+" with "+object);
        if (object == null) return false;
        if (!(object instanceof Row)) return false;
        Row row = (Row)object;
        return values.equals(row.values);
    }

    public int compareTo(Object object) {

        int c = 0;

        try {
            if (object == null) return 0;
            if (!(object instanceof Row)) return 0;

            Row row = (Row)object;

            Iterator i = values.keySet().iterator();
            Iterator j = row.values.keySet().iterator();

            while (i.hasNext() && j.hasNext()) {
                String name1 = (String)i.next();
                String name2 = (String)j.next();

                c = name1.compareTo(name2);
                if (c != 0) return c;

                Object value1 = values.get(name1);
                Object value2 = row.values.get(name2);

                if (value1 instanceof Comparable && value2 instanceof Comparable) {
                    Comparable v1 = (Comparable)value1;
                    Comparable v2 = (Comparable)value2;

                    c = v1.compareTo(v2);
                    if (c != 0) return c;
                }
            }

            if (i.hasNext()) return 1;
            if (j.hasNext()) return -1;

        } finally {
            //System.out.println("Comparing "+this+" with "+object+": "+c);
        }

        return c;
    }
}
