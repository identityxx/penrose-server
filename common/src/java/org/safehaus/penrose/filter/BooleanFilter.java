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
package org.safehaus.penrose.filter;

import java.util.Collection;

public class BooleanFilter extends Filter {

	boolean value;

	public BooleanFilter(boolean value) {
		this.value = value;
	}

	public boolean getValue() {
		return value;
	}

	public void setValue(boolean value) {
		this.value = value;
	}

    public int hashCode() {
        return value ? 0 : 1;
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        BooleanFilter simpleFilter = (BooleanFilter)object;
        if (value != simpleFilter.value) return false;

        return true;
    }

	public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("(");
        sb.append(value);
        sb.append(")");

        return sb.toString();
	}

    public String toString(Collection<Object> args) {
        StringBuilder sb = new StringBuilder();

        sb.append("(");
        sb.append(value);
        sb.append(")");

        return sb.toString();
    }

    public Object clone() throws CloneNotSupportedException {
        BooleanFilter filter = (BooleanFilter)super.clone();

        filter.value = value;

        return filter;
    }

    public boolean matches(Filter filter) throws Exception {
        if (filter == null) return false;
        if (filter == this) return true;
        if (filter.getClass() != getClass()) return false;

        return true;
    }
}