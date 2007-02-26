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

import org.ietf.ldap.LDAPDN;

import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;

/**
 * This class holds source's column values. Each value is an single object, not necessarily a collection.
 *
 * @author Endi S. Dewata
 */
public class RDN implements Comparable {

    public Map values = new TreeMap();

    public RDN() {
    }

    public RDN(Map values) {
        this.values.putAll(values);
    }

    public RDN(RDN rdn) {
        values.putAll(rdn.getValues());
    }

    public boolean isEmpty() {
        return values.isEmpty();
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

    public Object get(String name) {
        return values.get(name);
    }

    public Object remove(String name) {
        return values.remove(name);
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
            String rdn = name+"="+value;

            if (sb.length() > 0) sb.append("+");
            sb.append(LDAPDN.escapeRDN(rdn));
        }

        return sb.toString();
    }

    public int hashCode() {
        //System.out.println("RDN "+values+" hash code: "+values.hashCode());
        return values.hashCode();
    }

    public boolean equals(Object object) {
        //System.out.println("Comparing row "+values+" with "+object);
        if (object == null) return false;
        if (!(object instanceof RDN)) return false;
        RDN rdn = (RDN)object;
        return values.equals(rdn.values);
    }

    public int compareTo(Object object) {

        int c = 0;

        try {
            if (object == null) return 0;
            if (!(object instanceof RDN)) return 0;

            RDN rdn = (RDN)object;

            Iterator i = values.keySet().iterator();
            Iterator j = rdn.values.keySet().iterator();

            while (i.hasNext() && j.hasNext()) {
                String name1 = (String)i.next();
                String name2 = (String)j.next();

                c = name1.compareTo(name2);
                if (c != 0) return c;

                Object value1 = values.get(name1);
                Object value2 = rdn.values.get(name2);

                if (value1 instanceof Comparable && value2 instanceof Comparable) {
                    Comparable v1 = (Comparable)value1.toString();
                    Comparable v2 = (Comparable)value2.toString();

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
