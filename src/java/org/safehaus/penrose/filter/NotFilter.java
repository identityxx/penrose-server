/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.filter;

public class NotFilter extends Filter {

	protected Filter filter;

	public NotFilter() {
		super();
	}
	
	public NotFilter(Filter filter) {
		this.filter = filter;
	}

	public Filter getFilter() {
		return filter;
	}
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	public String toString() {
		return "(!" + filter.toString() + ")";
	}
}
