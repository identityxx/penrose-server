/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.openldap.config;

import java.io.StringReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.Penrose;


/**
 * @author Administrator
 */
public class NameValueItem extends ConfigurationItem {

    Logger log = LoggerFactory.getLogger(getClass());

	protected String name;
	protected String whitespace;
	protected String value;

	/**
	 * 
	 */
	public NameValueItem() {
		super();
	}

	/**
	 * @param originalText
	 */
	public NameValueItem(String originalText) {
		super(originalText);
		parse(originalText);
	}
	
	/**
	 * 
	 * @param s
	 */
	protected void parse(String s) {
		StringReader sr = new StringReader(s);
		SlapdParser parser = new SlapdParser(sr);
		try {
			String[] result = parser.NameValue();
			this.name = result[0];
			this.whitespace = result[1];
			this.value = result[2];
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		}
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
		modify();
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
		modify();
	}
	public String getWhitespace() {
		return whitespace;
	}
	public void setWhitespace(String whitespace) {
		this.whitespace = whitespace;
		modify();
	}
	
	protected void modify() {
		modifiedText = name + whitespace;
		if (value != null) {
			modifiedText +=
			("".equals(value) || value.indexOf(" ")>0 ? "\"" + value + "\"" : value);
		}
	}
}
