/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.filter;

import java.util.ArrayList;
import java.util.List;

public class SubstringFilter extends Filter {

	protected String attr;
	protected List substrings = new ArrayList();
	
	public SubstringFilter() {
		super();
	}
	
	public SubstringFilter(String attr, List substrings) {
		this.attr = attr;
		this.substrings = substrings;
	}
	
	public String getAttr() {
		return attr;
	}
	public void setAttr(String attr) {
		this.attr = attr;
	}
	public List getSubstrings() {
		return substrings;
	}
	public void setSubstrings(List substrings) {
		this.substrings = substrings;
	}
	public void addSubstring(String s) {
		this.substrings.add(s);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer("(" + attr + "=");
		for (int i=0; substrings != null && i < substrings.size(); i++) {
			sb.append(substrings.get(i));
		}
		sb.append(")");
		return sb.toString();
	}
}
