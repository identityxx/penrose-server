/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;

/**
 * @author Endi S. Dewata
 */
public class GenericModuleMapping implements ModuleMapping {

    public Logger log = Logger.getLogger(Penrose.MODULE_LOGGER);

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
     * Compare dn and dn2
     * @param dn
     * @param dn2
     * @return true if dn == dn2
     * @throws Exception
     */
    public boolean match(String dn, String dn2) throws Exception {

        //log.debug("Matching ["+dn+"] with ["+dn2+"]");
        int p = dn.indexOf("=");
        int q = dn.indexOf(",");

        String attr = dn.substring(0, p);
        String value = q >= 0 ? dn.substring(p+1, q) : dn.substring(p+1);
        String parent = q >= 0 ? dn.substring(q+1) : null;

        p = dn2.indexOf("=");
        q = dn2.indexOf(",");

        String attr2 = dn2.substring(0, p);
        String value2 = q >= 0 ? dn2.substring(p+1, q) : dn2.substring(p+1);
        String parent2 = q >= 0 ? dn2.substring(q+1) : null;

        // if attribute types don't match => false
        //log.debug(" - Comparing attribute types ["+attr+"] with ["+attr2+"]");
        if (!attr.equals(attr2)) return false;

        // if values are not dynamic and they don't match => false
        //log.debug(" - Comparing attribute values ["+value+"] with ["+value2+"]");
        if (!"...".equals(value) && !"...".equals(value2) && !value.equals(value2)) return false;

        // if parents matches => true
        //log.debug(" - Comparing parents ["+parent+"] with ["+parent2+"]");
        if (parent != null && parent2 != null && parent.equals(parent2)) return true;

        // if neither has parents => true
        return parent == null && parent2 == null;
    }

    public boolean match(String dn) throws Exception {

        if ("OBJECT".equals(scope)) {

            //log.debug("Matching object ["+baseDn+"] with ["+dn+"]");
            if (match(baseDn, dn)) return true;

        } else if ("ONELEVEL".equals(scope)) {

            //log.debug("Matching onelevel ["+baseDn+"] with ["+dn+"]");
            int i = dn.indexOf(",");
            String parent = dn.substring(i+1);
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
}