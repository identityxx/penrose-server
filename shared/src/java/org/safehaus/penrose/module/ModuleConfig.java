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

import java.util.Collection;
import java.util.Properties;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class ModuleConfig implements ModuleConfigMBean, Cloneable {

    private String name;
    private boolean enabled = true;
    private String moduleClass;
    private String description;

    public Properties parameters = new Properties();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void clearParameters() {
        parameters.clear();
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public Map getParameters() {
        return parameters;
    }

    public String getModuleClass() {
        return moduleClass;
    }

    public void setModuleClass(String moduleClass) {
        this.moduleClass = moduleClass;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (enabled ? 0 : 1) +
                (moduleClass == null ? 0 : moduleClass.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || object.getClass() != getClass()) return false;

        ModuleConfig moduleConfig = (ModuleConfig)object;
        if (!equals(name, moduleConfig.name)) return false;
        if (enabled != moduleConfig.enabled) return false;
        if (!equals(moduleClass, moduleConfig.moduleClass)) return false;
        if (!equals(description, moduleConfig.description)) return false;
        if (!equals(parameters, moduleConfig.parameters)) return false;

        return true;
    }

    public void copy(ModuleConfig moduleConfig) {
        name = moduleConfig.name;
        enabled = moduleConfig.enabled;
        moduleClass = moduleConfig.moduleClass;
        description = moduleConfig.description;

        parameters.clear();
        parameters.putAll(moduleConfig.parameters);
    }

    public Object clone() {
        ModuleConfig moduleConfig = new ModuleConfig();
        moduleConfig.copy(this);
        return moduleConfig;
    }
}
