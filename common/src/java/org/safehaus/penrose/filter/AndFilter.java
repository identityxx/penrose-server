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
import java.util.Iterator;
import java.util.List;

public class AndFilter extends Filter implements ContainerFilter {
	
	List<Filter> filters = new ArrayList<Filter>();

	public AndFilter() {
	}

    public AndFilter(Filter filter1, Filter filter2) {
        addFilter(filter1);
        addFilter(filter2);
    }

	public Collection<Filter> getFilters() {
		return filters;
	}

	public void addFilter(Filter filter) {
        if (filter == null) return;
        this.filters.add(filter);
        filter.setParent(this);
    }

    public void setFilter(int i, Filter filter) {
        if (filter == null) return;
		this.filters.set(i, filter);
        filter.setParent(this);
    }

    public Filter getFilter(int i) {
        return filters.get(i);
    }

    public boolean contains(Filter filter) {
        return filters.contains(filter);
    }

    public void replace(Filter oldFilter, Filter newFilter) {
        int i = filters.indexOf(oldFilter);
        if (i < 0) return;

        if (newFilter == null) {
            filters.remove(i);

            if (filters.size() == 1) {
                parent.replace(this, filters.get(0));

            } else if (filters.isEmpty()) {
                parent.replace(this, null);
            }

        } else if (newFilter instanceof BooleanFilter) {
            BooleanFilter bf = (BooleanFilter)newFilter;

            if (bf.getValue()) {
                filters.remove(i);

                if (filters.size() == 1) {
                    parent.replace(this, filters.get(0));

                } else if (filters.isEmpty()) {
                    parent.replace(this, bf);
                }

            } else {
                parent.replace(this, bf);
            }

        } else {
            filters.set(i, newFilter);
            newFilter.setParent(this);
        }
    }
    
    public int getSize() {
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
        if (object == null) return false;
        if (object == this) return true;
        if (object.getClass() != getClass()) return false;

        AndFilter andFilter = (AndFilter)object;
        if (!equals(filters, andFilter.filters)) return false;

        return true;
    }

	public String toString() {
		StringBuilder sb = new StringBuilder("(&");
        for (Filter filter : filters) {
            sb.append(filter);
        }
        sb.append(")");
		return sb.toString();
	}

    public String toString(Collection<Object> args) {
        StringBuilder sb = new StringBuilder("(&");
        for (Filter filter : filters) {
            sb.append(filter.toString(args));
        }
        sb.append(")");
        return sb.toString();
    }

    public Object clone() throws CloneNotSupportedException {
        AndFilter newAndFilter = (AndFilter)super.clone();

        newAndFilter.filters = new ArrayList<Filter>();

        for (Filter filter : filters) {
            newAndFilter.addFilter((Filter)filter.clone());
        }

        return newAndFilter;
    }

    public boolean matches(Filter filter) throws Exception {
        if (filter == null) return false;
        if (filter == this) return true;
        if (filter.getClass() != getClass()) return false;

        AndFilter f = (AndFilter)filter;

        if (filters.size() != f.filters.size()) return false;

        for (int i=0; i<filters.size(); i++) {
            Filter f1 = filters.get(i);
            Filter f2 = f.filters.get(i);

            if (!f1.matches(f2)) return false;
        }

        return true;
    }
}
