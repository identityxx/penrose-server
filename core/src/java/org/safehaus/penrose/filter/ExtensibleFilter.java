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

public class ExtensibleFilter extends ItemFilter {

	String attribute;
	String matchingRule;
	Object value;
	
	public ExtensibleFilter() {
	}
	
	public ExtensibleFilter(String attr, String matchingRule, Object value) {
		this.attribute = attr;
		this.matchingRule = matchingRule;
		this.value = value;
	}
	
	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public String getMatchingRule() {
		return matchingRule;
	}

	public void setMatchingRule(String matchingRule) {
		this.matchingRule = matchingRule;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

    public int hashCode() {
        return (attribute == null ? 0 : attribute.hashCode()) +
                (matchingRule == null ? 0 : matchingRule.hashCode()) +
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

        ExtensibleFilter extensibleFilter = (ExtensibleFilter)object;
        if (!equals(attribute, extensibleFilter.attribute)) return false;
        if (!equals(matchingRule, extensibleFilter.matchingRule)) return false;
        if (!equals(value, extensibleFilter.value)) return false;

        return true;
    }
	
	public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("(");
        if (attribute != null) sb.append(attribute);
        sb.append(":dn:");
        sb.append(matchingRule);
        sb.append(":={");
        sb.append(value);
        sb.append("})");

        return sb.toString();
	}

    public String toString(Collection<Object> args) {
        StringBuilder sb = new StringBuilder();

        sb.append("(");
        if (attribute != null) sb.append(attribute);
        sb.append(":dn:");
        sb.append(matchingRule);
        sb.append(":={");
        sb.append(args.size());
        sb.append("})");

        args.add(value);

        return sb.toString();
    }

    public Object clone() throws CloneNotSupportedException {
        ExtensibleFilter filter = (ExtensibleFilter)super.clone();

        filter.attribute = attribute;
        filter.matchingRule = matchingRule;
        filter.value = value;

        return filter;
    }
}
