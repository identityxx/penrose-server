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
import java.util.Iterator;
import java.util.Collection;

public class OrFilter extends Filter {

	Collection filters = new ArrayList();

	public OrFilter() {
	}

	public Collection getFilters() {
		return filters;
	}

	public void addFilter(Filter filter) {
		this.filters.add(filter);
	}

    public boolean containsFilter(Filter filter) {
        return filters.contains(filter);
    }

    public int size() {
        return filters.size();
    }

    public int hashCode() {
        return (filters == null ? 0 : filters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        OrFilter orFilter = (OrFilter)object;
        if (!equals(filters, orFilter.filters)) return false;

        return true;
    }

	public String toString() {
		StringBuilder sb = new StringBuilder("(|");
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter filter = (Filter)i.next();
            sb.append(filter);
        }
		sb.append(")");
		return sb.toString();
	}
}
