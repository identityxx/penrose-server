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

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class ModuleConfig implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    private boolean enabled = true;

    private String name;
    private String description;

    private String moduleClass;

    public Map<String,String> parameters = new LinkedHashMap<String,String>();

    public Collection<ModuleMapping> moduleMappings = new LinkedHashSet<ModuleMapping>();

    public ModuleConfig() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String,String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }

    public void clearParameters() {
        parameters.clear();
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

    public void addModuleMapping(ModuleMapping moduleMapping) {
        moduleMappings.add(moduleMapping);
    }

    public void removeModuleMapping(ModuleMapping moduleMapping) {
        Collection<ModuleMapping> list = new ArrayList<ModuleMapping>();
        for (ModuleMapping mm : moduleMappings) {
            if (!mm.equals(moduleMapping)) continue;
            list.add(mm);
        }
        moduleMappings.removeAll(list);
    }

    public void removeModuleMappings() {
        moduleMappings.clear();
    }
    
    public Collection<ModuleMapping> getModuleMappings() {
        return moduleMappings;
    }

    public void setModuleMappings(Collection<ModuleMapping> moduleMappings) {
        if (this.moduleMappings == moduleMappings) return;
        this.moduleMappings.clear();
        if (moduleMappings == null) return;
        this.moduleMappings.addAll(moduleMappings);
    }
    
    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        ModuleConfig moduleConfig = (ModuleConfig)object;
        if (enabled != moduleConfig.enabled) return false;

        if (!equals(name, moduleConfig.name)) return false;
        if (!equals(description, moduleConfig.description)) return false;

        if (!equals(moduleClass, moduleConfig.moduleClass)) return false;

        if (!equals(parameters, moduleConfig.parameters)) return false;

        if (!equals(moduleMappings, moduleConfig.moduleMappings)) return false;

        return true;
    }

    public void copy(ModuleConfig moduleConfig) throws CloneNotSupportedException {
        enabled = moduleConfig.enabled;

        name = moduleConfig.name;
        description = moduleConfig.description;

        moduleClass = moduleConfig.moduleClass;

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(moduleConfig.parameters);

        moduleMappings = new LinkedHashSet<ModuleMapping>();
        for (ModuleMapping moduleMapping : moduleConfig.moduleMappings) {
            moduleMappings.add((ModuleMapping)moduleMapping.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {
        ModuleConfig moduleConfig = (ModuleConfig)super.clone();
        moduleConfig.copy(this);
        return moduleConfig;
    }
}
