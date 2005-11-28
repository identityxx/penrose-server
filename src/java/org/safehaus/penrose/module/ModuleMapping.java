/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.module;

import org.apache.log4j.Logger;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Row;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModuleMapping implements Cloneable {

    public final static String OBJECT   = "OBJECT";
    public final static String ONELEVEL = "ONELEVEL";
    public final static String SUBTREE  = "SUBTREE";

    Logger log = Logger.getLogger(getClass());

    private String moduleName;
    private ModuleConfig moduleConfig;

    private String baseDn;
    private String filter;
    private String scope;

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getBaseDn() {
        return baseDn;
    }

    public void setBaseDn(String dnPattern) {
        this.baseDn = dnPattern;
    }
    
    
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public String getScope() {
		return scope;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}

    /**
     * Compare dn1 and dn2
     * @param dn1
     * @param dn2
     * @return true if dn1 == dn2
     * @throws Exception
     */
    public boolean match(String dn1, String dn2) throws Exception {

        //log.debug("Matching ["+dn1+"] with ["+dn2+"]");
        Row rdn1 = Entry.getRdn(dn1);
        Row rdn2 = Entry.getRdn(dn2);

        // if attribute types don't match => false
        //log.debug(" - Comparing attribute types ["+attr+"] with ["+attr2+"]");
        if (!rdn1.getNames().equals(rdn2.getNames())) return false;

        // if values are not dynamic and they don't match => false
        for (Iterator i=rdn1.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = (String)rdn1.get(name);
            String value2 = (String)rdn2.get(name);
            //log.debug(" - Comparing attribute values ["+value+"] with ["+value2+"]");
            if (!"...".equals(value) && !"...".equals(value2) && !value.equals(value2)) return false;
        }

        String parentDn1 = Entry.getParentDn(dn1);
        String parentDn2 = Entry.getParentDn(dn2);

        // if parents matches => true
        //log.debug(" - Comparing parents ["+parentDn1+"] with ["+parentDn2+"]");
        if (parentDn1 != null && parentDn2 != null && parentDn1.equals(parentDn2)) return true;

        // if neither has parents => true
        return parentDn1 == null && parentDn2 == null;
    }

    public boolean match(String dn) throws Exception {

        if ("OBJECT".equals(scope)) {

            //log.debug("Matching object ["+baseDn+"] with ["+dn+"]");
            if (match(baseDn, dn)) return true;

        } else if ("ONELEVEL".equals(scope)) {

            //log.debug("Matching onelevel ["+baseDn+"] with ["+dn+"]");
            String parent = Entry.getParentDn(dn);
            if (match(baseDn, parent)) return true;

        } else if ("SUBTREE".equals(scope)) {

            //log.debug("Matching subtree ["+baseDn+"] with ["+dn+"]");
            String a = baseDn;
            String b = dn;

            int i;
            int j;

            do {
                i = a.lastIndexOf(",");
                j = b.lastIndexOf(",");

                String c = a.substring(i+1);
                String d = b.substring(j+1);

                if (!match(c, d)) return false;

                if (i >= 0) a = a.substring(0, i);
                if (j >= 0) b = b.substring(0, j);

            } while (i >=0 && j >= 0);

            // if basedn has been exhausted => true
            return i < 0;
        }

        return false;
    }

    public ModuleConfig getModuleConfig() {
        return moduleConfig;
    }

    public void setModuleConfig(ModuleConfig moduleConfig) {
        this.moduleConfig = moduleConfig;
    }

    public int hashCode() {
        return moduleName.hashCode() + baseDn.hashCode() + scope.hashCode() + filter.hashCode();
    }

    public boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof ModuleMapping)) return false;

        ModuleMapping mapping = (ModuleMapping)o;
        if (!equals(moduleName, mapping.getModuleName())) return false;
        if (!equals(baseDn, mapping.getBaseDn())) return false;
        if (!equals(scope, mapping.getScope())) return false;
        if (!equals(filter, mapping.getFilter())) return false;
        
        return true;
    }

    public Object clone() {
        ModuleMapping mapping = new ModuleMapping();
        mapping.moduleName = moduleName;
        mapping.moduleConfig = moduleConfig == null ? null : (ModuleConfig)moduleConfig.clone();
        mapping.baseDn = baseDn;
        mapping.filter = filter;
        mapping.scope = scope;
        return mapping;
    }
}
