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

public class NotFilter extends Filter implements ContainerFilter {

	Filter filter;

	public NotFilter(Filter filter) {
		this.filter = filter;
        filter.setParent(this);
    }

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
        filter.setParent(this);
	}

    public void replace(Filter oldFilter, Filter newFilter) {
        if (oldFilter != filter) return;

        if (newFilter == null) {
            if (parent != null) {
                parent.replace(this, null);
            }

        } else if (newFilter instanceof BooleanFilter) {
            BooleanFilter bf = (BooleanFilter)newFilter;
            bf.setValue(!bf.getValue());

            parent.replace(this, bf);

        } else {
            filter = newFilter;
            newFilter.setParent(this);
        }
    }

    public int hashCode() {
        return (filter == null ? 0 : filter.hashCode());
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

        NotFilter notFilter = (NotFilter)object;
        if (!equals(filter, notFilter.filter)) return false;

        return true;
    }

	public String toString() {
		StringBuilder sb = new StringBuilder();

        sb.append("(!");
        sb.append(filter);
        sb.append(")");

        return sb.toString();
    }

    public String toString(Collection<Object> args) {
        StringBuilder sb = new StringBuilder();

        sb.append("(!");
        sb.append(filter.toString(args));
        sb.append(")");

        return sb.toString();
    }

    public Object clone() throws CloneNotSupportedException {
        NotFilter newFilter = (NotFilter)super.clone();

        newFilter.setFilter((Filter)filter.clone());

        return newFilter;
    }

    public boolean matches(Filter filter) throws Exception {
        if (filter == null) return false;
        if (filter == this) return true;
        if (filter.getClass() != getClass()) return false;

        NotFilter f = (NotFilter)filter;

        return this.filter.matches(f);
    }
}
