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

public class ExtensibleFilter extends Filter {

	protected String attribute;
	protected String matchingRule;
	protected String value;
	
	public ExtensibleFilter() {
	}
	
	public ExtensibleFilter(String attr, String matchingRule, String value) {
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

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
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
        if((object == null) || (object.getClass() != this.getClass())) return false;

        ExtensibleFilter extensibleFilter = (ExtensibleFilter)object;
        if (!equals(attribute, extensibleFilter.attribute)) return false;
        if (!equals(matchingRule, extensibleFilter.matchingRule)) return false;
        if (!equals(value, extensibleFilter.value)) return false;

        return true;
    }
	
	public String toString() {
		return "(" + (attribute != null ? attribute : "") +
		":dn:" + matchingRule + ":=" + value + ")";
	}
}
