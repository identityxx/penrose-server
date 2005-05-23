/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.openldap.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * The slapd.conf(5) file consists of three types of 
 * configuration information: global, penrose specific,
 * and database specific. Global information is specified 
 * first, followed by information associated with a particular 
 * penrose type, which is then followed by information
 * associated with a particular database instance. 
 * Global directives can be overridden in penrose and/or
 * database directives, and penrose directives can be
 * overridden by database directives.
 * 
 * Blank lines and comment lines beginning with a '#' character 
 * are ignored. If a line begins with white space, it is 
 * considered a continuation of the previous line (even if the 
 * previous line is a comment).
 * 
 * See also: http://www.openldap.org/doc/admin22/slapdconfig.html
 * 
 * <pre>
 * Example usage:
 * 
 * Reader in = ...;
 * SlapdConfig config = new SlapdConfig(in);
 * // to print out the configuration text
 * System.out.println(config.toString());
 * // listing out all the config items
 * List items = config.getItems();
 * for (int i=0; i<items.size(); i++) {
 *   ConfigurationItem ci = (ConfigurationItem) items.get(i);
 *   ...
 * }
 * </pre>
 */
public class SlapdConfig {

	protected final static String NEWLINE = System.getProperty("line.separator");
	
	protected final static String[] NAME_VALUE_CONFIGS = {
			"database", "suffix", "rootdn", "rootpw", "classpath",
			"libpath", "trustedKeyStore", "serverConfig", 
			"mappingConfig", "include", "homeDirectory", "className" };
	
	protected List items = new ArrayList();
	
	/**
	 * 
	 */
	public SlapdConfig(Reader r) throws IOException {
		super();
		parse(r);
	}
	
	public void parse(Reader r) throws IOException {
		BufferedReader br = new BufferedReader(r);
		String line = br.readLine();
		String item = line;
		ConfigurationItem ci = null;
		
		while(true) {
			line = br.readLine();
			if (line == null) {
				// If no more line to be processed,
				// process the remaining item
				ci = process(item);
				items.add(ci);
				break;
			} else if (line.startsWith(" ") || line.startsWith("\t")) {
				// If a line begins with white space
				// it is considered a continuation of the 
				// previous line (even if the previous line 
				// is a comment).
				item += NEWLINE + line;
			} else {
				// If the line does not start with white space
				// it is considered a new item, so we 
				// - process the item
				ci = process(item);
				items.add(ci);
				// - begin new item
				item = line;
			}
		}
	}
	
	public ConfigurationItem process(String item) {
		ConfigurationItem ci = null;
		
		if (item == null || item.length() == 0) {
			ci = new BlankLine(item);
		} else if (item.startsWith("#")) {
			ci = new Comment(item);
		} else {
			for (int i=0; i<NAME_VALUE_CONFIGS.length; i++) {
				if (item.startsWith(NAME_VALUE_CONFIGS[i])) {
					ci = new NameValueItem(item);
				}
			}
			if (ci == null) {
				ci = new ConfigurationItem(item);
			}
		}
		return ci;
	}

	
	public List getItems() {
		return items;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<items.size(); i++) {
			sb.append(items.get(i).toString() + NEWLINE);
		}
		return sb.toString();
	}

}
