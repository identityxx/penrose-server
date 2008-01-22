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
package org.safehaus.penrose.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;

public class SubstringFilter extends ItemFilter {

    public final static Character STAR = '*';

	String attribute;
	Collection<Object> substrings = new ArrayList<Object>();
	
	public SubstringFilter(String attribute, Object[] substrings) {
        this(attribute, Arrays.asList(substrings));
	}
	
	public SubstringFilter(String attribute, Collection<Object> substrings) {
		this.attribute = attribute;
        this.substrings.addAll(substrings);
	}
	
	public String getAttribute() {
		return attribute;
	}

    public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

    public Collection<Object> getSubstrings() {
		return substrings;
	}

	public void addSubstring(String s) {
		this.substrings.add(s);
	}

    public int hashCode() {
        return (attribute == null ? 0 : attribute.hashCode()) +
                (substrings == null ? 0 : substrings.hashCode());
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

        SubstringFilter substringFilter = (SubstringFilter)object;
        if (!equals(attribute, substringFilter.attribute)) return false;
        if (!equals(substrings, substringFilter.substrings)) return false;

        return true;
    }

	public String toString() {
		StringBuilder sb = new StringBuilder("(" + attribute + "=");
        for (Object o : substrings) {
            if (o.equals(STAR)) {
                sb.append(o);
            } else {
                String value = (String) o;
                sb.append(FilterTool.escape(value));
            }
        }
        sb.append(")");
		return sb.toString();
	}

    public String toString(Collection<Object> args) {
        StringBuilder sb = new StringBuilder("(" + attribute + "=");
        for (Object o : substrings) {
            if (o.equals(STAR)) {
                sb.append(o);
            } else {
                String value = (String) o;
                sb.append("{");
                sb.append(args.size());
                sb.append("}");
                args.add(value);
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public Object clone() throws CloneNotSupportedException {
        SubstringFilter filter = (SubstringFilter)super.clone();

        filter.attribute = attribute;

        filter.substrings = new ArrayList<Object>();
        filter.substrings.addAll(substrings);

        return filter;
    }
}
