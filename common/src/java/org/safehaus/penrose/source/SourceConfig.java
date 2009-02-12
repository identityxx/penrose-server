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
package org.safehaus.penrose.source;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class SourceConfig implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(SourceConfig.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    public final static String REFRESH_METHOD          = "refreshMethod";
    public final static String RELOAD_EXPIRED          = "reloadExpired";
    public final static String POLL_CHANGES            = "pollChanges";

    public final static String LOAD_ON_STARTUP         = "loadOnStartup";
    public final static String LOAD_UPON_EXPIRATION    = "loadUponExpiration";

    public final static String QUERY_CACHE_SIZE        = "queryCacheSize";
    public final static String QUERY_CACHE_EXPIRATION  = "queryCacheExpiration";

    public final static String DATA_CACHE_SIZE         = "dataCacheSize";
    public final static String DATA_CACHE_EXPIRATION   = "dataCacheExpiration";

    public final static String SIZE_LIMIT              = "sizeLimit";
    public final static String TIME_LIMIT              = "timeLimit";

    public final static String CACHE                   = "cache";

    public final static boolean DEFAULT_AUTO_REFRESH           = false;
    public final static String DEFAULT_REFRESH_METHOD          = POLL_CHANGES;

    public final static int    DEFAULT_QUERY_CACHE_SIZE        = 100;
    public final static int    DEFAULT_QUERY_CACHE_EXPIRATION  = 5;

    public final static int    DEFAULT_DATA_CACHE_SIZE         = 100;
    public final static int    DEFAULT_DATA_CACHE_EXPIRATION   = 5;

    public final static int    DEFAULT_SIZE_LIMIT              = 0;
    public final static int    DEFAULT_TIME_LIMIT              = 0;

    public boolean enabled = true;

	public String name;
    public String description;
    public String sourceClass;

    public String partitionName;
    public String adapterName;
    public String connectionName;

    public Map<String,String> parameters = new LinkedHashMap<String,String>();

    public Map<String,FieldConfig> fieldConfigs = new LinkedHashMap<String,FieldConfig>();
    public Map<String,FieldConfig> fieldConfigsByOriginalName = new LinkedHashMap<String,FieldConfig>();

    public Collection<FieldConfig> pkFieldConfigs = new ArrayList<FieldConfig>();
    public Collection<FieldConfig> nonPkFieldConfigs = new ArrayList<FieldConfig>();

    public Map<String,IndexConfig> indexConfigs = new LinkedHashMap<String,IndexConfig>();

    public SourceConfig() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSourceClass() {
        return sourceClass;
    }

    public void setSourceClass(String sourceClass) {
        this.sourceClass = sourceClass;
    }

    public FieldConfig getFieldConfig(String name) {
        return fieldConfigs.get(name);
    }

    public FieldConfig getFieldConfigByOriginalName(String originalName) {
        return fieldConfigsByOriginalName.get(originalName);
    }

    public Collection<String> getPrimaryKeyNames() {
        Collection<String> results = new LinkedHashSet<String>();
        for (FieldConfig fieldConfig : pkFieldConfigs) {
            results.add(fieldConfig.getName());
        }
        return results;
    }

    public Collection<String> getOriginalPrimaryKeyNames() {
        Collection<String> results = new LinkedHashSet<String>();
        for (FieldConfig fieldConfig : pkFieldConfigs) {
            results.add(fieldConfig.getOriginalName());
        }
        return results;
    }

    public Collection<FieldConfig> getPrimaryKeyFieldConfigs() {
        return pkFieldConfigs;
    }

    public Collection<FieldConfig> getNonPrimaryKeyFieldConfigs() {
        return nonPkFieldConfigs;
    }

    public Collection<FieldConfig> getUniqueFieldConfigs() {
        Collection<FieldConfig> results = new ArrayList<FieldConfig>();
        for (FieldConfig fieldConfig : fieldConfigs.values()) {
            if (!fieldConfig.isUnique()) continue;
            results.add(fieldConfig);
        }
        return results;
    }

    public Collection<String> getIndexFieldNames() {
        Collection<String> results = new LinkedHashSet<String>();
        for (FieldConfig fieldConfig : fieldConfigs.values()) {
            if (!fieldConfig.isPrimaryKey() && !fieldConfig.isUnique() && !fieldConfig.isIndex()) continue;
            results.add(fieldConfig.getName());
        }
        return results;
    }

    public Collection<FieldConfig> getIndexedFieldConfigs() {
        Collection<FieldConfig> results = new ArrayList<FieldConfig>();
        for (FieldConfig fieldConfig : fieldConfigs.values()) {
            if (!fieldConfig.isPrimaryKey() && !fieldConfig.isUnique() && !fieldConfig.isIndex()) continue;
            results.add(fieldConfig);
        }
        return results;
    }

    public Collection<String> getFieldNames() {
        return fieldConfigs.keySet();
    }
    
    public Collection<String> getFieldOriginalNames() {
        Collection<String> results = new ArrayList<String>();
        for (FieldConfig fieldConfig : fieldConfigs.values()) {
            results.add(fieldConfig.getOriginalName());
        }
        return results;
    }

    public Collection<FieldConfig> getFieldConfigs() {
		return fieldConfigs.values();
	}

    public  void setFieldConfigs(Collection<FieldConfig> fieldConfigs) throws Exception {
        this.fieldConfigs.clear();
        fieldConfigsByOriginalName.clear();
        pkFieldConfigs.clear();
        nonPkFieldConfigs.clear();

        for (FieldConfig fieldConfig : fieldConfigs) {
            addFieldConfig((FieldConfig)fieldConfig.clone());
        }
    }

    public void addFieldConfig(FieldConfig fieldConfig) {
        String name = fieldConfig.getName();
        //log.debug("Adding field "+name+" ("+fieldConfig.isPrimaryKey()+")");

        fieldConfigs.put(name, fieldConfig);
        fieldConfigsByOriginalName.put(fieldConfig.getOriginalName(), fieldConfig);
        if (fieldConfig.isPrimaryKey()) {
            pkFieldConfigs.add(fieldConfig);
        } else {
            nonPkFieldConfigs.add(fieldConfig);
        }
    }

    public void renameFieldConfig(String oldName, String newName) {
        if (oldName.equals(newName)) return;

        FieldConfig fieldConfig = fieldConfigs.get(oldName);
        if (fieldConfig == null) return;

        fieldConfigs.remove(oldName);
        fieldConfigs.put(newName, fieldConfig);
    }

    public void modifySourceConfig(String name, FieldConfig newFieldConfig) throws Exception {
        FieldConfig fieldConfig = fieldConfigs.get(name);
        fieldConfig.copy(newFieldConfig);
    }

    public void removeFieldConfig(FieldConfig fieldConfig) {
        fieldConfigs.remove(fieldConfig.getName());
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameter(String name, String value) {
        if (value == null) {
            parameters.remove(name);
        } else {
            parameters.put(name, value);
        }
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Map<String,String> getParameters() {
        return parameters;
    }
    
    public void setParameters(Map<String,String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public void setAdapterName(String adapterName) {
        this.adapterName = adapterName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Collection<IndexConfig> getIndexConfigs() {
        return indexConfigs.values();
    }

    public void addIndexConfig(IndexConfig indexConfig) {
        String indexName = indexConfig.getName();
        if (indexName == null) {
            int i = 0;
            indexName = name+"_index"+i;
            while (indexConfigs.containsKey(indexName)) {
                i++;
                indexName = name+"_index"+i;
            }
            //indexConfig.setName(indexName);
        }
        indexConfigs.put(indexName, indexConfig);
    }

    public void setIndexConfigs(Map<String,IndexConfig> indexConfigs) {
        if (this.indexConfigs == indexConfigs) return;
        this.indexConfigs.clear();
        this.indexConfigs.putAll(indexConfigs);
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

        SourceConfig sourceConfig = (SourceConfig)object;
        if (enabled != sourceConfig.enabled) return false;

        if (!equals(name, sourceConfig.name)) return false;
        if (!equals(description, sourceConfig.description)) return false;
        if (!equals(sourceClass, sourceConfig.sourceClass)) return false;

        if (!equals(partitionName, sourceConfig.partitionName)) return false;
        if (!equals(adapterName, sourceConfig.adapterName)) return false;
        if (!equals(connectionName, sourceConfig.connectionName)) return false;

        if (!equals(fieldConfigs, sourceConfig.fieldConfigs)) return false;
        if (!equals(parameters, sourceConfig.parameters)) return false;

        if (!equals(indexConfigs, sourceConfig.indexConfigs)) return false;

        return true;
    }

    public void copy(SourceConfig sourceConfig) throws CloneNotSupportedException {
        enabled = sourceConfig.enabled;

        name = sourceConfig.name;
        description = sourceConfig.description;
        sourceClass = sourceConfig.sourceClass;

        partitionName = sourceConfig.partitionName;
        adapterName = sourceConfig.adapterName;
        connectionName = sourceConfig.connectionName;

        fieldConfigs               = new LinkedHashMap<String,FieldConfig>();
        fieldConfigsByOriginalName = new LinkedHashMap<String,FieldConfig>();
        pkFieldConfigs             = new ArrayList<FieldConfig>();
        nonPkFieldConfigs          = new ArrayList<FieldConfig>();

        for (FieldConfig fieldConfig : sourceConfig.fieldConfigs.values()) {
            addFieldConfig((FieldConfig) fieldConfig.clone());
        }

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(sourceConfig.parameters);

        indexConfigs = new LinkedHashMap<String,IndexConfig>();

        for (IndexConfig indexConfig : sourceConfig.indexConfigs.values()) {
            addIndexConfig((IndexConfig) indexConfig.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {
        SourceConfig sourceConfig = (SourceConfig)super.clone();
        sourceConfig.copy(this);
        return sourceConfig;
    }
}