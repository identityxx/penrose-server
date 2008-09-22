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
package org.safehaus.penrose.directory;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class EntrySourceConfig implements Serializable, Cloneable {

    public final static String REQUIRED   = "required";
    public final static String REQUISITE  = "requisite";
    public final static String SUFFICIENT = "sufficient";
    public final static String IGNORE     = "ignore";
    public final static String OPTIONAL   = "optional";

    public final static String FILTER     = "filter";

	public String alias = "DEFAULT";

    public String partitionName;
    public String sourceName;

    public String mappingName;

	public Map<String,Collection<EntryFieldConfig>> fieldConfigs = new LinkedHashMap<String,Collection<EntryFieldConfig>>();

    public Map<String,String> parameters = new LinkedHashMap<String,String>();

    public boolean readOnly = false;

    public String search;
    public String bind;

    public String add;
    public String delete;
    public String modify;
    public String modrdn;

    public Integer searchOrder;
    public Integer bindOrder;
    public Integer addOrder;
    public Integer deleteOrder;
    public Integer modifyOrder;
    public Integer modrdnOrder;

    public EntrySourceConfig() {
	}

    public EntrySourceConfig(String alias, String sourceName) {
        this.alias = alias;
        this.sourceName = sourceName;
    }

    public EntrySourceConfig(String alias, String partitionName, String sourceName) {
        this.alias = alias;
        this.partitionName = partitionName;
        this.sourceName = sourceName;
    }

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

    public Collection<EntryFieldConfig> getFieldConfigs(String name) {
        return fieldConfigs.get(name.toLowerCase());
    }

	public Collection<EntryFieldConfig> getFieldConfigs() {
        Collection<EntryFieldConfig> results = new ArrayList<EntryFieldConfig>();
        for (Collection<EntryFieldConfig> list : fieldConfigs.values()) {
            results.addAll(list);
        }
        return results;
	}

	public void addFieldConfig(EntryFieldConfig fieldConfig) {
        String name = fieldConfig.getName().toLowerCase();
        Collection<EntryFieldConfig> list = fieldConfigs.get(name);
        if (list == null) {
            list = new ArrayList<EntryFieldConfig>();
            fieldConfigs.put(name, list);
        }
        list.add(fieldConfig);
	}

    public void removeFieldConfigs(String name) {
        fieldConfigs.remove(name.toLowerCase());
    }

    public void removeFieldConfig(EntryFieldConfig fieldConfig) {
        Collection<EntryFieldConfig> list = getFieldConfigs(fieldConfig.getName());
        if (list == null) return;

        list.remove(fieldConfig);
        if (list.isEmpty()) removeFieldConfigs(fieldConfig.getName());
    }

    public void removeFieldConfigs() {
        fieldConfigs.clear();
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getSourceName() {
        return sourceName == null ? alias : sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void removeParameters() {
        parameters.clear();
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public int hashCode() {
        return (alias == null ? 0 : alias.hashCode()) +
                (partitionName == null ? 0 : partitionName.hashCode()) +
                (sourceName == null ? 0 : sourceName.hashCode());
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

        EntrySourceConfig sourceConfig = (EntrySourceConfig)object;
        if (!equals(alias, sourceConfig.alias)) return false;
        if (!equals(partitionName, sourceConfig.partitionName)) return false;
        if (!equals(sourceName, sourceConfig.sourceName)) return false;
        if (!equals(mappingName, sourceConfig.mappingName)) return false;
        if (!equals(fieldConfigs, sourceConfig.fieldConfigs)) return false;
        if (!equals(parameters, sourceConfig.parameters)) return false;
        if (readOnly != sourceConfig.readOnly) return false;

        if (!equals(search, sourceConfig.search)) return false;
        if (!equals(bind, sourceConfig.bind)) return false;
        if (!equals(add, sourceConfig.add)) return false;
        if (!equals(delete, sourceConfig.delete)) return false;
        if (!equals(modify, sourceConfig.modify)) return false;
        if (!equals(modrdn, sourceConfig.modrdn)) return false;

        if (!equals(searchOrder, sourceConfig.searchOrder)) return false;
        if (!equals(bindOrder, sourceConfig.bindOrder)) return false;
        if (!equals(addOrder, sourceConfig.addOrder)) return false;
        if (!equals(deleteOrder, sourceConfig.deleteOrder)) return false;
        if (!equals(modifyOrder, sourceConfig.modifyOrder)) return false;
        if (!equals(modrdnOrder, sourceConfig.modrdnOrder)) return false;

        return true;
    }

    public void copy(EntrySourceConfig sourceConfig) throws CloneNotSupportedException {
        alias = sourceConfig.alias;
        partitionName = sourceConfig.partitionName;
        sourceName = sourceConfig.sourceName;
        mappingName = sourceConfig.mappingName;

        fieldConfigs = new LinkedHashMap<String,Collection<EntryFieldConfig>>();
        for (Collection<EntryFieldConfig> list : sourceConfig.fieldConfigs.values()) {
            for (EntryFieldConfig fieldConfig : list) {
                addFieldConfig((EntryFieldConfig) fieldConfig.clone());
            }
        }

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(sourceConfig.parameters);

        readOnly = sourceConfig.readOnly;

        search = sourceConfig.search;
        bind = sourceConfig.bind;
        add = sourceConfig.add;
        delete = sourceConfig.delete;
        modify = sourceConfig.modify;
        modrdn = sourceConfig.modrdn;

        searchOrder = sourceConfig.searchOrder;
        bindOrder = sourceConfig.bindOrder;
        addOrder = sourceConfig.addOrder;
        deleteOrder = sourceConfig.deleteOrder;
        modifyOrder = sourceConfig.modifyOrder;
        modrdnOrder = sourceConfig.modrdnOrder;
    }

    public Object clone() throws CloneNotSupportedException {
        EntrySourceConfig sourceConfig = (EntrySourceConfig)super.clone();
        sourceConfig.copy(this);
        return sourceConfig;
    }

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public String getAdd() {
        return add;
    }

    public void setAdd(String add) {
        this.add = add;
    }

    public String getDelete() {
        return delete;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }

    public String getModify() {
        return modify;
    }

    public void setModify(String modify) {
        this.modify = modify;
    }

    public String getModrdn() {
        return modrdn;
    }

    public void setModrdn(String modrdn) {
        this.modrdn = modrdn;
    }

    public String getSearch() {
        return search;
    }

    public void setSearch(String search) {
        this.search = search;
    }

    public Integer getSearchOrder() {
        return searchOrder;
    }

    public void setSearchOrder(Integer searchOrder) {
        this.searchOrder = searchOrder;
    }

    public void setSearchOrder(String searchOrder) {
        this.searchOrder = Integer.parseInt(searchOrder);
    }

    public Integer getBindOrder() {
        return bindOrder;
    }

    public void setBindOrder(Integer bindOrder) {
        this.bindOrder = bindOrder;
    }

    public void setBindOrder(String bindOrder) {
        this.bindOrder = Integer.parseInt(bindOrder);
    }

    public Integer getAddOrder() {
        return addOrder;
    }

    public void setAddOrder(Integer addOrder) {
        this.addOrder = addOrder;
    }

    public void setAddOrder(String addOrder) {
        this.addOrder = Integer.parseInt(addOrder);
    }

    public Integer getDeleteOrder() {
        return deleteOrder;
    }

    public void setDeleteOrder(Integer deleteOrder) {
        this.deleteOrder = deleteOrder;
    }

    public void setDeleteOrder(String deleteOrder) {
        this.deleteOrder = Integer.parseInt(deleteOrder);
    }

    public Integer getModifyOrder() {
        return modifyOrder;
    }

    public void setModifyOrder(Integer modifyOrder) {
        this.modifyOrder = modifyOrder;
    }

    public void setModifyOrder(String modifyOrder) {
        this.modifyOrder = Integer.parseInt(modifyOrder);
    }

    public Integer getModrdnOrder() {
        return modrdnOrder;
    }

    public void setModrdnOrder(Integer modrdnOrder) {
        this.modrdnOrder = modrdnOrder;
    }

    public void setModrdnOrder(String modrdnOrder) {
        this.modrdnOrder = Integer.parseInt(modrdnOrder);
    }
}