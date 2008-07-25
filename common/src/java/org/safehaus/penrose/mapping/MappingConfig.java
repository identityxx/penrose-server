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
package org.safehaus.penrose.mapping;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class MappingConfig implements Serializable, Cloneable {

    private boolean enabled = true;

    protected String name;
    protected String mappingClass;
    protected String description;

    protected String preScript;
    protected String postScript;

    protected List<MappingFieldConfig> fieldConfigs = new LinkedList<MappingFieldConfig>();
    protected Map<String,List<MappingFieldConfig>> fieldConfigsByName = new TreeMap<String,List<MappingFieldConfig>>();

    protected Map<String,String> parameters = new TreeMap<String,String>();

    public MappingConfig() {
	}

    public MappingConfig(String name) {
        this.name = name;
    }

    public Collection<String> getFieldNames() {
        return fieldConfigsByName.keySet();
    }
    
    public Collection<MappingFieldConfig> getFieldConfigs() {
        return fieldConfigs;
    }

    public void removeFieldConfigs() {
        fieldConfigs.clear();
        fieldConfigsByName.clear();
    }

    public void addFieldConfig(MappingFieldConfig mappingFieldConfig) {

        String name = mappingFieldConfig.getName();

        fieldConfigs.add(mappingFieldConfig);

        List<MappingFieldConfig> list = fieldConfigsByName.get(name);
        if (list == null) {
            list = new LinkedList<MappingFieldConfig>();
            fieldConfigsByName.put(name, list);
        }
        list.add(mappingFieldConfig);
    }

    public void addFieldConfig(int index, MappingFieldConfig mappingFieldConfig) {

        String name = mappingFieldConfig.getName();

        fieldConfigs.add(index, mappingFieldConfig);

        List<MappingFieldConfig> list = fieldConfigsByName.get(name);
        if (list == null) {
            list = new LinkedList<MappingFieldConfig>();
            fieldConfigsByName.put(name, list);
        }

        if (list.isEmpty()) {
            list.add(mappingFieldConfig);
        } else {
            for (MappingFieldConfig fc : list) {
                int i = fieldConfigs.indexOf(fc);
                if (i < index) continue;
                list.add(i, mappingFieldConfig);
            }
        }
    }

    public int getMappingFieldConfigIndex(MappingFieldConfig mappingFieldConfig) {
        return fieldConfigs.indexOf(mappingFieldConfig);
    }
    
    public Collection<MappingFieldConfig> getMappingFieldConfigs(String name) {
        return fieldConfigsByName.get(name);
    }

    public Collection<MappingFieldConfig> removeMappingFieldConfigs(String name) {
         return fieldConfigsByName.remove(name);
    }

    public void removeMappingFieldConfig(MappingFieldConfig mappingFieldConfig) {

        String name = mappingFieldConfig.getName();

        fieldConfigs.remove(mappingFieldConfig);

        Collection<MappingFieldConfig> list = fieldConfigsByName.get(name);
        if (list == null) return;

        list.remove(mappingFieldConfig);
        if (list.isEmpty()) fieldConfigsByName.remove(name);
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String removeParameter(String name) {
        return parameters.remove(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
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
        if (object == this) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        MappingConfig mappingConfig = (MappingConfig)object;
        if (enabled != mappingConfig.enabled) return false;

        if (!equals(name, mappingConfig.name)) return false;

        if (!equals(mappingClass, mappingConfig.mappingClass)) return false;
        if (!equals(description, mappingConfig.description)) return false;

        if (!equals(preScript, mappingConfig.preScript)) return false;
        if (!equals(postScript, mappingConfig.postScript)) return false;

        if (!equals(fieldConfigs, mappingConfig.fieldConfigs)) return false;

        if (!equals(parameters, mappingConfig.parameters)) return false;

        return true;
    }

    public void copy(MappingConfig mappingConfig) throws CloneNotSupportedException {
        enabled = mappingConfig.enabled;

        name = mappingConfig.name;

        mappingClass = mappingConfig.mappingClass;
        description = mappingConfig.description;

        preScript = mappingConfig.preScript;
        postScript = mappingConfig.postScript;

        fieldConfigs = new LinkedList<MappingFieldConfig>();
        fieldConfigsByName = new TreeMap<String,List<MappingFieldConfig>>();

        for (MappingFieldConfig fieldMapping : mappingConfig.fieldConfigs) {
            addFieldConfig((MappingFieldConfig)fieldMapping.clone());
        }

        parameters = new TreeMap<String,String>();
        parameters.putAll(mappingConfig.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        MappingConfig mappingConfig = (MappingConfig)super.clone();
        mappingConfig.copy(this);
        return mappingConfig;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMappingClass() {
        return mappingClass;
    }

    public void setMappingClass(String mappingClass) {
        this.mappingClass = mappingClass;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getPreScript() {
        return preScript;
    }

    public void setPreScript(String preScript) {
        this.preScript = preScript;
    }

    public String getPostScript() {
        return postScript;
    }

    public void setPostScript(String postScript) {
        this.postScript = postScript;
    }
}