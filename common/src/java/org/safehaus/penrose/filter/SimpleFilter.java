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

import java.util.Collection;

public class SimpleFilter extends ItemFilter {

	String attribute;
	String operator;
	Object value;

	public SimpleFilter(String attribute, String operator, Object value) {
		this.attribute = attribute;
		this.operator = operator;
		this.value = value;
	}
	
	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

    public int hashCode() {
        return (attribute == null ? 0 : attribute.hashCode()) +
                (operator == null ? 0 : operator.hashCode()) +
                (value == null ? 0 : value.hashCode());
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

        SimpleFilter simpleFilter = (SimpleFilter)object;
        if (!equals(attribute, simpleFilter.attribute)) return false;
        if (!equals(operator, simpleFilter.operator)) return false;
        if (!equals(value, simpleFilter.value)) return false;

        return true;
    }

	public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("(");
        sb.append(attribute);
        sb.append(operator);
        sb.append(FilterTool.escape(value));
        sb.append(")");

        return sb.toString();
	}

    public String toString(Collection<Object> args) {
        StringBuilder sb = new StringBuilder();

        sb.append("(");
        sb.append(attribute);
        sb.append(operator);
        sb.append("{");
        sb.append(args.size());
        sb.append("}");
        sb.append(")");

        args.add(value);

        return sb.toString();
    }

    public Object clone() throws CloneNotSupportedException {
        SimpleFilter filter = (SimpleFilter)super.clone();

        filter.attribute = attribute;
        filter.operator = operator;
        filter.value = value;

        return filter;
    }

    public boolean matches(Filter filter) throws Exception {
        if (filter == null) return false;
        if (filter == this) return true;
        if (filter.getClass() != getClass()) return false;

        SimpleFilter f = (SimpleFilter)filter;

        String name1 = attribute.toLowerCase();
        String name2 = f.attribute.toLowerCase();

        if (!"...".equals(name1) && !"...".equals(name2) && !name1.equals(name2)) return false;

        if (!operator.equals(f.operator)) return false;

        String value1 = value.toString().toLowerCase();
        String value2 = f.value.toString().toLowerCase();

        if (!"...".equals(value1) && !"...".equals(value2) && !value1.equals(value2)) return false;

        return true;
    }
}
