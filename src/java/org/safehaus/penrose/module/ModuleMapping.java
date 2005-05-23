/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

/**
 * @author Endi S. Dewata
 */
public interface ModuleMapping {
	
	public final static String OBJECT   = "OBJECT";
	public final static String ONELEVEL = "ONELEVEL";
	public final static String SUBTREE  = "SUBTREE";
	
    public String getModuleName();
    public String getBaseDn();
    public String getFilter();
    public String getScope();

    public boolean match(String dn) throws Exception;
}
