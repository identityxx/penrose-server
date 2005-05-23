/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.filter;

public class PresentFilter extends Filter {

	protected String attr;
	
	public PresentFilter() {
		super();
	}
	
	public PresentFilter(String attr) {
		this.attr = attr;
	}
	
	public String getAttr() {
		return attr;
	}
	public void setAttr(String attr) {
		this.attr = attr;
	}
	
	public String toString() {
		return "(" + attr + "=*)";
	}
}
