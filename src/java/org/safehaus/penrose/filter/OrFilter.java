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

import java.util.ArrayList;
import java.util.List;

public class OrFilter extends Filter {

	protected List filterList = new ArrayList();

	public OrFilter() {
		super();
	}

	public List getFilterList() {
		return filterList;
	}
	public void setFilterList(List filterList) {
		this.filterList = filterList;
	}
	public void addFilterList(Filter filter) {
		this.filterList.add(filter);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer("(|");
		for (int i=0; filterList != null && i<filterList.size(); i++) {
			sb.append(filterList.get(i));
		}
		sb.append(")");
		return sb.toString();
	}

    public int size() {
        return filterList.size();
    }
}
