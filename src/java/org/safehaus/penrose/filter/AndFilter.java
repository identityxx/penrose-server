/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.filter;

import java.util.ArrayList;
import java.util.List;

public class AndFilter extends Filter {
	
	protected List filterList = new ArrayList();

	public AndFilter() {
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
		StringBuffer sb = new StringBuffer("(&");
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
