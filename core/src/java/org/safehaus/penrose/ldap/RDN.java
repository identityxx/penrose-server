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
package org.safehaus.penrose.ldap;

import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;
import java.io.Serializable;

/**
 * This class holds source's column values. Each value is an single object, not necessarily a collection.
 *
 * @author Endi S. Dewata
 */
public class RDN implements Serializable, Comparable {

    public Map<String,Object> values = new TreeMap<String,Object>();

    protected String original;
    protected String normalized;
    public String pattern;

    public RDN() {
    }

    public RDN(String rdn) {
        values = new DN(rdn).getRdn().values;
    }

    public RDN(Map<String,Object> values) {
        this.values.putAll(values);
    }

    public RDN(RDN rdn) {
        values.putAll(rdn.getValues());
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Object get(String name) {
        return values.get(name);
    }

    public Collection<String> getNames() {
        return values.keySet();
    }

    public boolean contains(String name) {
        return values.containsKey(name);
    }

    public Map<String,Object> getValues() {
        return values;
    }

    public String getOriginal() {
        if (original != null) return original;
        original = buildString(false);
        return original;
    }

    public String getNormalized() {
        if (normalized != null) return normalized;
        normalized = buildString(true);
        return normalized;
    }

    private String buildString(boolean normalize) {
        StringBuilder sb = new StringBuilder();
        for (String name : values.keySet()) {
            Object value = values.get(name);
            if (value == null) continue;

            if (sb.length() > 0) sb.append('+');
            sb.append(normalize ? name.toLowerCase() : name);
            sb.append('=');
            if (value instanceof byte[]) {
                sb.append('#');
                byte[] bytes = (byte[]) value;
                for (byte b : bytes) {
                    sb.append(hex((char)b));
                }
            } else {
                sb.append(LDAP.escape(normalize ? value.toString().toLowerCase() : value.toString()));
            }
        }

        return sb.toString();
    }

    private static String hex(char b) {
        String hex = Integer.toHexString(b);
        if (hex.length() % 2 == 1) {
            hex = "0" + hex;
        }
        return hex;
    }

    public int hashCode() {
        return getOriginal().hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        RDN rdn = (RDN)object;
        if (!equals(getOriginal(), rdn.getOriginal())) return false;

        return true;
    }

    public int compareTo(Object object) {

        if (object == null) return 0;
        if (!(object instanceof RDN)) return 0;

        RDN rdn = (RDN)object;

        Iterator i = values.keySet().iterator();
        Iterator j = rdn.values.keySet().iterator();

        while (i.hasNext() && j.hasNext()) {
            String name1 = (String)i.next();
            String name2 = (String)j.next();

            int c = name1.compareTo(name2);
            if (c != 0) return c;

            Object value1 = values.get(name1);
            Object value2 = rdn.values.get(name2);

            if (value1 instanceof Comparable && value2 instanceof Comparable) {
                String v1 = value1.toString();
                String v2 = value2.toString();

                c = v1.compareTo(v2);
                if (c != 0) return c;
            }
        }

        if (i.hasNext()) return 1;
        if (j.hasNext()) return -1;

        return 0;
    }

    public String toString() {
        return getOriginal();
    }

    public String getPattern() {
        if (pattern == null) createPattern(0);
        return pattern;
    }

    public int createPattern(int counter) {

        StringBuilder sb = new StringBuilder();
        for (String name : values.keySet()) {
            String value = (String) values.get(name);

            if (sb.length() > 0) sb.append('+');

            sb.append(name);
            sb.append('=');

            if ("...".equals(value)) {
                sb.append('{');
                sb.append(counter++);
                sb.append('}');

            } else {
                sb.append(value);
            }
        }

        pattern = sb.toString();

        return counter;
    }

    public boolean matches(RDN rdn) {

        if (rdn == null) return false;
        if (getNormalized().equals(rdn.getNormalized())) return true;

        Collection names = values.keySet();
        Collection names2 = rdn.values.keySet();
        if (names.size() != names2.size()) return false;

        Iterator i = names.iterator();
        Iterator j = names2.iterator();

        while (i.hasNext() && j.hasNext()) {
            String name = (String)i.next();
            String name2 = (String)j.next();

            if (!name.equalsIgnoreCase(name2)) {
                return false;
            }

            Object value = values.get(name);
            Object value2 = rdn.values.get(name2);

            if ("...".equals(value)) continue;
            if ("...".equals(value2)) continue;

            String s1 = value.toString();
            String s2 = value2.toString();

            if (!s1.equalsIgnoreCase(s2)) return false;
        }

        return true;
    }
}
