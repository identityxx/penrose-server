/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import org.safehaus.penrose.ldap.DN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class ModuleMapping implements Cloneable {

    public final static String OBJECT   = "OBJECT";
    public final static String ONELEVEL = "ONELEVEL";
    public final static String SUBTREE  = "SUBTREE";

    Logger log = LoggerFactory.getLogger(getClass());

    private String moduleName;
    private ModuleConfig moduleConfig;

    private DN baseDn;
    private String filter;
    private String scope;

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public DN getBaseDn() {
        return baseDn;
    }

    public void setBaseDn(String baseDn) {
        setBaseDn(new DN(baseDn));
    }
    
    public void setBaseDn(DN baseDn) {
        this.baseDn = baseDn;
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

    public boolean match(DN dn) throws Exception {

        if ("OBJECT".equals(scope)) {

            //log.debug("Matching object ["+baseDn+"] with ["+dn+"]");
            if (baseDn.matches(dn)) return true;

        } else if ("ONELEVEL".equals(scope)) {

            //log.debug("Matching onelevel ["+baseDn+"] with ["+dn+"]");
            DN parentDn = dn.getParentDn();
            if (baseDn.matches(parentDn)) return true;

        } else if ("SUBTREE".equals(scope)) {

            return dn.endsWith(baseDn);
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

    public Object clone() throws CloneNotSupportedException {
        super.clone();
        ModuleMapping mapping = new ModuleMapping();
        mapping.moduleName = moduleName;
        mapping.moduleConfig = moduleConfig == null ? null : (ModuleConfig)moduleConfig.clone();
        mapping.baseDn = baseDn;
        mapping.filter = filter;
        mapping.scope = scope;
        return mapping;
    }
}
