/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.filter;

public class SimpleFilter extends Filter {

	protected String attr;
	protected String filterType;
	protected String value;
	
	public SimpleFilter() {
		super();
	}
	
	public SimpleFilter(String attr, String filterType, String value) {
		this.attr = attr;
		this.filterType = filterType;
		this.value = value;
	}
	
	public String getAttr() {
		return attr;
	}
	public void setAttr(String attr) {
		this.attr = attr;
	}
	public String getFilterType() {
		return filterType;
	}
	public void setFilterType(String filterType) {
		this.filterType = filterType;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	public String toString() {
		return "(" + attr + filterType + value + ")";
	}
}
