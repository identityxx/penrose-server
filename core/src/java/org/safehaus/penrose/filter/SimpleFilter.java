/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

public class SimpleFilter extends Filter {

	protected String attribute;
	protected String operator;
	protected String value;

	public SimpleFilter(String attribute, String operator, String value) {
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

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
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
        if((object == null) || (object.getClass() != this.getClass())) return false;

        SimpleFilter simpleFilter = (SimpleFilter)object;
        if (!equals(attribute, simpleFilter.attribute)) return false;
        if (!equals(operator, simpleFilter.operator)) return false;
        if (!equals(value, simpleFilter.value)) return false;

        return true;
    }

	public String toString() {
		return "(" + attribute + operator + value + ")";
	}
}
