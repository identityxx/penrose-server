/**
 * Copyright 2009 Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.ldap;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.safehaus.penrose.util.BinaryUtil;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class SourceAttributes implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    public Map<String,Attributes> map = new TreeMap<String,Attributes>();

    public SourceAttributes() {
    }

    public void clear(String sourceName) {
        Attributes attributes = get(sourceName);
        attributes.clear();
    }
    
    public void set(SourceAttributes sourceAttributes) throws Exception {
        if (sourceAttributes == null) return;

        for (String sourceName : sourceAttributes.map.keySet()) {
            Attributes attributes = sourceAttributes.map.get(sourceName);
            set(sourceName, attributes);
        }
    }

    public void set(String sourceName, SearchResult result) {
        clear(sourceName);
        set(sourceName, "dn", result.getDn().toString());
        add(sourceName, result.getAttributes());
    }

    public void add(String sourceName, SearchResult result) {
        Attributes attributes = get(sourceName);
        attributes.addValue("dn", result.getDn().toString());
        attributes.add(result.getAttributes());
    }

    public void set(String sourceName, String attributeName, Object attributeValue) {
        Attributes attributes = get(sourceName);
        attributes.setValue(attributeName, attributeValue);
    }

    public void set(String sourceName, Attributes attributes) throws Exception {
        map.put(sourceName, (Attributes)attributes.clone());
    }

    public void add(String sourceName, Attributes newAttributes) {

        Attributes attributes = map.get(sourceName);
        if (attributes == null) {
            attributes = new Attributes();
            map.put(sourceName, attributes);
        }

        attributes.add(newAttributes);
    }

    public void add(SourceAttributes sourceAttributes) {
        if (sourceAttributes == null) return;

        for (String sourceName : sourceAttributes.map.keySet()) {
            Attributes attributes = sourceAttributes.map.get(sourceName);
            add(sourceName, attributes);
        }
    }

    public void remove(String sourceName) {
        if (sourceName == null) return;

        Collection<String> list = new ArrayList<String>();
        list.addAll(map.keySet());

        for (String s : list) {
            if (s.equals(sourceName) || s.startsWith(sourceName + ".")) {
                map.remove(s);
            }
        }
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Attributes get(String sourceName) {

        Attributes attributes = map.get(sourceName);

        if (attributes == null) {
            attributes = new Attributes();
            map.put(sourceName, attributes);
        }

        return attributes;
    }

    public Attribute get(String sourceName, String attributeName) {
        Attributes attributes = get(sourceName);
        return attributes.get(attributeName);
    }

    public Object getValue(String sourceName, String attributeName) {
        Attributes attributes = get(sourceName);
        return attributes.getValue(attributeName);
    }

    public Collection<Object> getValues(String sourceName, String attributeName) {
        Attributes attributes = get(sourceName);
        return attributes.getValues(attributeName);
    }

    public Collection<String> getNames() {
        return map.keySet();
    }

    public boolean contains(String name) {
        boolean b = map.containsKey(name);
        if (b) return true;

        for (String s : map.keySet()) {
            if (s.startsWith(name + ".")) return true;
        }

        return false;
    }

    public void clear() {
        map.clear();
    }

    public String toString() {
        return map.toString();
    }


    public int hashCode() {
        return map.hashCode();
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        SourceAttributes av = (SourceAttributes)object;
        if (!map.equals(av.map)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        SourceAttributes object = (SourceAttributes)super.clone();

        object.map = new TreeMap<String,Attributes>();

        for (String sourceName : map.keySet()) {
            Attributes attributes = map.get(sourceName);
            object.map.put(sourceName, (Attributes)attributes.clone());
        }

        return object;
    }
/*
    public int compareTo(Object object) {

        int c = 0;

        if (object == null) return 0;
        if (!(object instanceof SourceValues)) return 0;

        SourceValues sourceValues = (SourceValues)object;

        Iterator i = map.keySet().iterator();
        Iterator j = sourceValues.map.keySet().iterator();

        while (i.hasNext() && j.hasNext()) {
            String name1 = (String)i.next();
            String name2 = (String)j.next();

            c = name1.compareTo(name2);
            if (c != 0) return c;

            Attributes values1 = map.get(name1);
            Attributes values2 = sourceValues.map.get(name2);

            Iterator k = values1.iterator();
            Iterator l = values2.iterator();

            while (k.hasNext() && l.hasNext()) {
                Object value1 = k.next();
                Object value2 = l.next();

                if (value1 instanceof Comparable && value2 instanceof Comparable) {
                    Comparable<String> v1 = value1.toString();
                    String v2 = value2.toString();

                    c = v1.compareTo(v2);
                    if (c != 0) return c;
                }
            }

            if (k.hasNext()) return 1;
            if (l.hasNext()) return -1;
        }

        if (i.hasNext()) return 1;
        if (j.hasNext()) return -1;

        return c;
    }
*/
    public void print() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());

        for (String sourceName : map.keySet()) {
            Attributes attributes = map.get(sourceName);

            for (String fieldName : attributes.getNames()) {
                Attribute attribute = attributes.get(fieldName);

                for (Object value : attribute.getValues()) {

                    String className = value.getClass().getName();
                    className = className.substring(className.lastIndexOf(".") + 1);

                    if (value instanceof byte[]) {
                        value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[]) value, 0, 10)+"...";
                    }

                    log.debug(" - " + sourceName + "." + fieldName + ": " + value + " (" + className + ")");
                }
            }
        }
    }
}
