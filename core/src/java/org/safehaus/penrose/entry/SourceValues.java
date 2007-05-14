/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.entry;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.ldap.RDN;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceValues implements Cloneable, Comparable {

    Logger log = LoggerFactory.getLogger(getClass());

    public Map<String,Collection<Object>> values = new TreeMap<String,Collection<Object>>();

    public SourceValues() {
    }

    public SourceValues(SourceValues sourceValues) {
        add(sourceValues);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }
    
    public void add(SourceValues sourceValues) {
        add(null, sourceValues);
    }

    public void add(String prefix, SourceValues sourceValues) {
        if (sourceValues == null) return;
        Map<String,Collection<Object>> v = sourceValues.getValues();
        for (String name : v.keySet()) {
            Collection<Object> c = v.get(name);
            add(prefix == null ? name : prefix + "." + name, c);
        }
    }

    public void shift(String prefix) {
        Map<String,Collection<Object>> newValues = new TreeMap<String,Collection<Object>>();
        for (String name : values.keySet()) {
            Collection<Object> c = values.get(name);
            newValues.put(prefix + "." + name, c);
        }
        values = newValues;
    }

    public void add(RDN rdn) {
        add(null, rdn);
    }

    public void add(String prefix, RDN rdn) {
        if (rdn == null) return;
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            String targetName = prefix == null ? name : prefix + "." + name;
            Collection<Object> c = get(targetName);
            if (c == null) c = new LinkedHashSet<Object>();
            c.add(value);
            set(targetName, c);
        }
    }

    public void set(RDN rdn) {
        set(null, rdn);
    }

    public void set(String prefix, RDN rdn) {
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);

            String targetName = prefix == null ? name : prefix + "." + name;
            Collection<Object> c = new LinkedHashSet<Object>();
            c.add(value);
            set(targetName, c);
        }
    }

    public void set(SourceValues sourceValues) {
        set(null, sourceValues);
    }

    public void set(String prefix, SourceValues sourceValues) {
        if (sourceValues == null) {
            remove(prefix);
            return;
        }

        for (String name : sourceValues.getNames()) {
            Collection<Object> c = sourceValues.get(name);
            set(prefix == null ? name : prefix + "." + name, c);
        }
    }

    public void set(String name, Collection<Object> values) {
        Collection<Object> c = new LinkedHashSet<Object>();
        c.addAll(values);
        this.values.put(name, c);
    }

    public void set(String name, Object value) {

        if (value == null) return;

        if (value instanceof Collection) {
            set(name, (Collection)value);
            return;
        }

        Collection<Object> c = new LinkedHashSet<Object>();
        c.add(value);
        this.values.put(name, c);
    }

    public void add(String name, Object value) {

        if (value == null) return;

        if (value instanceof Collection) {
            add(name, (Collection)value);
            return;
        }
        
        Collection<Object> c = this.values.get(name);
        if (c == null) {
            c = new LinkedHashSet<Object>();
            this.values.put(name, c);
        }
        c.add(value);
    }

    public void add(String name, Object objects[]) {
        add(name, Arrays.asList(objects));
    }

    public void add(String name, Collection<Object> values) {
        if (values == null) return;
        Collection<Object> c = this.values.get(name);
        if (c == null) {
            c = new LinkedHashSet<Object>();
            this.values.put(name, c);
        }
        c.addAll(values);
    }

    public Collection<Object> get(String name) {
        return values.get(name);
    }

    public Object getOne(String name) {
        Collection<Object> c = values.get(name);
        if (c == null || c.isEmpty()) return null;
        return c.iterator().next();
    }
    
    public Collection<String> getNames() {
        return values.keySet();
    }

    public boolean contains(String name) {
        boolean b = values.containsKey(name);
        if (b) return true;

        for (String s : values.keySet()) {
            if (s.startsWith(name + ".")) return true;
        }

        return false;
    }

    public boolean contains(RDN rdn) {
        return contains(null, rdn);
    }
    
    public boolean contains(String prefix, RDN rdn) {

        for (String name : rdn.getNames()) {
            String fullName = (prefix == null ? name : prefix + "." + name);

            Object value = rdn.get(name);
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
        if (name == null) return;

        Collection<String> list = new ArrayList<String>();
        list.addAll(values.keySet());

        for (String s : list) {
            if (s.equals(name) || s.startsWith(name + ".")) {
                values.remove(s);
            }
        }
    }

    public void remove(String name, Object value) {
        if (value == null) return;

        Collection<Object> c = values.get(name);
        if (c == null) return;

        c.remove(value);
        if (c.isEmpty()) values.remove(name);
    }

    public void remove(String name, Collection values) {
        if (values == null) return;

        Collection<Object> c = this.values.get(name);
        if (c == null) return;

        c.removeAll(values);
        if (c.isEmpty()) this.values.remove(name);
    }

    public void retain(Collection<String> names) {
        Collection<String> lcNames = new ArrayList<String>();
        for (String name : names) {
            lcNames.add(name.toLowerCase());
        }

        Collection<String> list = new ArrayList<String>();
        list.addAll(values.keySet());

        for (String s : list) {
            int p = s.indexOf(".");
            String n = p >= 0 ? s.substring(0, p) : s;

            if (!lcNames.contains(n.toLowerCase())) values.remove(s);
        }
    }

    public void retain(String name) {
        Collection<String> list = new ArrayList<String>();
        list.addAll(values.keySet());

        for (String s : list) {

            int p = s.indexOf(".");
            String n = p >= 0 ? s.substring(0, p) : s;

            if (!name.equalsIgnoreCase(n)) values.remove(s);
        }
    }
    
    public Map<String,Collection<Object>> getValues() {
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
        if (!(object instanceof SourceValues)) return false;
        SourceValues av = (SourceValues)object;
        return values.equals(av.values);
    }

    public Object clone() throws CloneNotSupportedException {
        super.clone();

        SourceValues sourceValues = new SourceValues();
        for (String name : values.keySet()) {
            Collection<Object> c =  values.get(name);
            Collection<Object> s = new LinkedHashSet<Object>();
            s.addAll(c);
            sourceValues.values.put(name, s);
        }
        return sourceValues;
    }

    public int compareTo(Object object) {

        int c = 0;

        try {
            if (object == null) return 0;
            if (!(object instanceof SourceValues)) return 0;

            SourceValues sourceValues = (SourceValues)object;

            Iterator i = values.keySet().iterator();
            Iterator j = sourceValues.values.keySet().iterator();

            while (i.hasNext() && j.hasNext()) {
                String name1 = (String)i.next();
                String name2 = (String)j.next();

                c = name1.compareTo(name2);
                if (c != 0) return c;

                Collection<Object> values1 = values.get(name1);
                Collection<Object> values2 = sourceValues.values.get(name2);

                Iterator k = values1.iterator();
                Iterator l = values2.iterator();

                while (k.hasNext() && l.hasNext()) {
                    Object value1 = k.next();
                    Object value2 = l.next();

                    if (value1 instanceof Comparable && value2 instanceof Comparable) {
                        Comparable v1 = value1.toString();
                        Comparable v2 = value2.toString();

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

    public void print() throws Exception {
        Logger log = LoggerFactory.getLogger(getClass());

        for (String name : values.keySet()) {
            Collection<Object> list = values.get(name);

            for (Object value : list) {
                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".") + 1);

                if (value instanceof byte[]) {
                    value = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[]) value);
                }

                log.debug(" - " + name + ": " + value + " (" + className + ")");
            }
        }
    }
}
