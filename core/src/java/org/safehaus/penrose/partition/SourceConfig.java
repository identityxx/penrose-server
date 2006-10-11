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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.AttributeValues;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceConfig implements SourceConfigMBean, Cloneable {

    Logger log = LoggerFactory.getLogger(getClass());

    //public final static String AUTO_REFRESH            = "autoRefresh";

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
    public final static String LOADING_METHOD          = "loadingMethod";

    public final static String LOAD_ALL                = "loadAll";
    public final static String SEARCH_AND_LOAD         = "searchAndLoad";

    public final static String CACHE                   = "cache";

    public final static boolean DEFAULT_AUTO_REFRESH           = false;
    public final static String DEFAULT_REFRESH_METHOD          = POLL_CHANGES;

    public final static int    DEFAULT_QUERY_CACHE_SIZE        = 100;
    public final static int    DEFAULT_QUERY_CACHE_EXPIRATION  = 5;

    public final static int    DEFAULT_DATA_CACHE_SIZE         = 100;
    public final static int    DEFAULT_DATA_CACHE_EXPIRATION   = 5;

    public final static int    DEFAULT_SIZE_LIMIT              = 0;
    public final static String DEFAULT_LOADING_METHOD          = LOAD_ALL;

    public final static String DEFAULT_CACHE                   = "DEFAULT";

	/**
	 * Name.
	 */
	private String name;

    private String connectionName;

    private String description;

    /**
     * Parameters.
     */
    private Properties parameters = new Properties();

    /**
     * Fields. Each element is of type org.safehaus.penrose.partition.FieldConfig.
     */
    private Map fieldConfigs = new TreeMap();

	public SourceConfig() {
	}

    public SourceConfig(String name, String connectionName) {
        this.name = name;
        this.connectionName = connectionName;
    }
    
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public FieldConfig getFieldConfig(String name) {
        return (FieldConfig)fieldConfigs.get(name);
    }

    public Collection getPrimaryKeyNames() {
        Collection results = new TreeSet();
        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (!fieldConfig.isPK()) continue;
            results.add(fieldConfig.getName());
        }
        return results;
    }

    public Collection getOriginalPrimaryKeyNames() {
        Collection results = new TreeSet();
        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (!fieldConfig.isPK()) continue;
            results.add(fieldConfig.getOriginalName());
        }
        return results;
    }

    public Collection getPrimaryKeyFieldConfigs() {
        Collection results = new ArrayList();
        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (!fieldConfig.isPK()) continue;
            results.add(fieldConfig);
        }
        return results;
    }

    public Collection getNonPrimaryKeyFieldConfigs() {
        Collection results = new ArrayList();
        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (fieldConfig.isPK()) continue;
            results.add(fieldConfig);
        }
        return results;
    }

    public Collection getUniqueFieldConfigs() {
        Collection results = new ArrayList();
        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (!fieldConfig.isUnique()) continue;
            results.add(fieldConfig);
        }
        return results;
    }

    public Collection getIndexedFieldConfigs() {
        Collection results = new ArrayList();
        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (!fieldConfig.isPK() && !fieldConfig.isUnique() && !fieldConfig.isIndex()) continue;
            results.add(fieldConfig);
        }
        return results;
    }

	public Collection getFieldConfigs() {
		return fieldConfigs.values();
	}

	public void addFieldConfig(FieldConfig fieldConfig) {
        String name = fieldConfig.getName();
        log.debug("Adding field "+name+(fieldConfig.isPK() ? " ("+fieldConfig.getPrimaryKey()+")" : ""));

        fieldConfigs.put(name, fieldConfig);
	}

    public void renameFieldConfig(String oldName, String newName) {
        if (oldName.equals(newName)) return;

        FieldConfig fieldConfig = (FieldConfig)fieldConfigs.get(oldName);
        if (fieldConfig == null) return;

        fieldConfigs.remove(oldName);
        fieldConfigs.put(newName, fieldConfig);
    }

    public void modifySourceConfig(String name, FieldConfig newFieldConfig) {
        FieldConfig fieldConfig = (FieldConfig)fieldConfigs.get(name);
        fieldConfig.copy(newFieldConfig);
    }

    public void removeFieldConfig(FieldConfig fieldConfig) {
        fieldConfigs.remove(fieldConfig.getName());
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Map getParameters() {
        return parameters;
    }
    
    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }


    public Row getPrimaryKeyValues(AttributeValues sourceValues) throws Exception {

        Row pk = new Row();

        for (Iterator i=fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            if (!fieldConfig.isPK()) continue;

            String fieldName = fieldConfig.getName();

            Collection values = sourceValues.get(fieldName);
            if (values == null) return null;

            Iterator iterator = values.iterator();
            if (!iterator.hasNext()) return null;

            Object value = iterator.next();

            pk.set(fieldName, value);
        }

        return pk;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (connectionName == null ? 0 : connectionName.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
                (fieldConfigs == null ? 0 : fieldConfigs.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        SourceConfig sourceConfig = (SourceConfig)object;
        if (!equals(name, sourceConfig.name)) return false;
        if (!equals(connectionName, sourceConfig.connectionName)) return false;
        if (!equals(description, sourceConfig.description)) return false;
        if (!equals(fieldConfigs, sourceConfig.fieldConfigs)) return false;
        if (!equals(parameters, sourceConfig.parameters)) return false;

        return true;
    }

    public void copy(SourceConfig sourceConfig) {
        name = sourceConfig.name;
        connectionName = sourceConfig.connectionName;
        description = sourceConfig.description;

        fieldConfigs.clear();
        for (Iterator i = sourceConfig.fieldConfigs.values().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)((FieldConfig)i.next()).clone();
            addFieldConfig((FieldConfig)fieldConfig.clone());
        }

        parameters.clear();
        parameters.putAll(sourceConfig.parameters);
    }

    public Object clone() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.copy(this);
        return sourceConfig;
    }
}