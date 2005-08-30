/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.openldap.config;

/**
 */
public class ConfigurationItem {

	protected String originalText;
	protected String modifiedText;
	
	/**
	 * 
	 */
	public ConfigurationItem() {
		super();
	}
	
	public ConfigurationItem(String originalText) {
		this.originalText = originalText;
		// this.modifiedText = originalText;
	}
	
	public String toString() {
		// return modifiedText;
		if (modifiedText == null) {
			return originalText;
		} else {
			return modifiedText;
		}
	}
	
}
