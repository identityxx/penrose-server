/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;

/**
 * This class holds source's column values. Each value is an single object, not necessarily a collection.
 *
 * @author Endi S. Dewata
 */
public class Row {

    public Map values = new TreeMap();

    public Row() {
    }

    public Row(Map values) {
        values.putAll(values);
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
        return values.toString();
    }
}
