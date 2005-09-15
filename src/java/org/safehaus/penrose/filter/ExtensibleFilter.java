/** * Copyright (c) 2000-2005, Identyx Corporation.
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.filter;public class ExtensibleFilter extends Filter {	protected String attr;	protected String matchingRule;	protected String value;		public ExtensibleFilter() {		super();	}		public ExtensibleFilter(String attr, String matchingRule, String value) {		this.attr = attr;		this.matchingRule = matchingRule;		this.value = value;	}		public String getAttr() {		return attr;	}	public void setAttr(String attr) {		this.attr = attr;	}	public String getMatchingRule() {		return matchingRule;	}	public void setMatchingRule(String matchingRule) {		this.matchingRule = matchingRule;	}	public String getValue() {		return value;	}	public void setValue(String value) {		this.value = value;	}		public String toString() {		return "(" + (attr != null ? attr : "") + 		":dn:" + matchingRule + ":=" + value + ")";	}}