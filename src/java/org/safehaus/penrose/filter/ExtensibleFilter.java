/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.filter;

public class ExtensibleFilter extends Filter {

	protected String attr;
	protected String matchingRule;
	protected String value;
	
	public ExtensibleFilter() {
		super();
	}
	
	public ExtensibleFilter(String attr, String matchingRule, String value) {
		this.attr = attr;
		this.matchingRule = matchingRule;
		this.value = value;
	}
	
	public String getAttr() {
		return attr;
	}
	public void setAttr(String attr) {
		this.attr = attr;
	}
	public String getMatchingRule() {
		return matchingRule;
	}
	public void setMatchingRule(String matchingRule) {
		this.matchingRule = matchingRule;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	public String toString() {
		return "(" + (attr != null ? attr : "") + 
		":dn:" + matchingRule + ":=" + value + ")";
	}
}
