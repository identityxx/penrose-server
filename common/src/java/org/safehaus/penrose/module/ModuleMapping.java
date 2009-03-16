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

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class ModuleMapping implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    static {
        log = LoggerFactory.getLogger(ModuleMapping.class);
    }

    public static transient Logger log;

    public final static String OBJECT   = "OBJECT";
    public final static String ONELEVEL = "ONELEVEL";
    public final static String SUBTREE  = "SUBTREE";

    private String moduleName;

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

        if (dn == null) {
            return false;

        } else if ("OBJECT".equals(scope)) {

            //log.debug("Matching object ["+baseDn+"] with ["+dn+"]");
            return baseDn.matches(dn);

        } else if ("ONELEVEL".equals(scope)) {

            //log.debug("Matching onelevel ["+baseDn+"] with ["+dn+"]");
            DN parentDn = dn.getParentDn();
            return baseDn.matches(parentDn);

        } else { // if ("SUBTREE".equals(scope)) {

            return dn.endsWith(baseDn);
        }
    }

    public int hashCode() {
        return (moduleName == null ? 0 : moduleName.hashCode())
                + (baseDn == null ? 0 : baseDn.hashCode())
                + (scope == null ? 0 : scope.hashCode())
                + (filter == null ? 0 : filter.hashCode());
    }

    public boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        ModuleMapping mapping = (ModuleMapping)object;
        if (!equals(moduleName, mapping.getModuleName())) return false;
        if (!equals(baseDn, mapping.getBaseDn())) return false;
        if (!equals(scope, mapping.getScope())) return false;
        if (!equals(filter, mapping.getFilter())) return false;

        return true;
    }

    public void copy(ModuleMapping mapping) throws CloneNotSupportedException {
        moduleName = mapping.moduleName;
        baseDn = mapping.baseDn;
        filter = mapping.filter;
        scope = mapping.scope;
    }

    public Object clone() throws CloneNotSupportedException {
        ModuleMapping mapping = (ModuleMapping)super.clone();
        mapping.copy(this);
        return mapping;
    }
}
