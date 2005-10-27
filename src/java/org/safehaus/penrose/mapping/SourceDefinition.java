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
package org.safehaus.penrose.mapping;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceDefinition implements Cloneable {

    public final static String AUTO_RELOAD             = "autoReload";

    public final static String LOAD_ON_STARTUP         = "loadOnStartup";
    public final static String LOAD_UPON_EXPIRATION    = "loadUponExpiration";

    public final static String FILTER_CACHE_SIZE       = "filterCacheSize";
    public final static String FILTER_CACHE_EXPIRATION = "filterCacheExpiration";

    public final static String DATA_CACHE_SIZE         = "dataCacheSize";
    public final static String DATA_CACHE_EXPIRATION   = "dataCacheExpiration";

    public final static String SIZE_LIMIT              = "sizeLimit";
    public final static String LOADING_METHOD          = "loadingMethod";

    public final static String LOAD_ALL                = "loadAll";
    public final static String SEARCH_AND_LOAD         = "searchAndLoad";

    public final static String CACHE                   = "cache";

    public final static boolean DEFAULT_AUTO_RELOAD            = false;

    public final static int    DEFAULT_FILTER_CACHE_SIZE       = 100;
    public final static int    DEFAULT_FILTER_CACHE_EXPIRATION = 5;

    public final static int    DEFAULT_DATA_CACHE_SIZE         = 100;
    public final static int    DEFAULT_DATA_CACHE_EXPIRATION   = 5;

    public final static int    DEFAULT_SIZE_LIMIT              = 100;
    public final static String DEFAULT_LOADING_METHOD          = LOAD_ALL;

    public final static String DEFAULT_CACHE                   = "SourceCache";

	/**
	 * Name.
	 */
	private String name;

    private String connectionName;

    private String description;

	/**
	 * Fields. Each element is of type org.safehaus.penrose.mapping.FieldDefinition.
	 */
	private Map fields = new TreeMap();

    /**
     * Parameters.
     */
    private Properties parameters = new Properties();

	public SourceDefinition() {
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public FieldDefinition getFieldDefinition(String name) {
        return (FieldDefinition)fields.get(name);
    }

    public Collection getPrimaryKeyFieldDefinitions() {
        Collection results = new ArrayList();
        for (Iterator i=fields.values().iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (!fieldDefinition.isPrimaryKey()) continue;
            results.add(fieldDefinition);
        }
        return results;
    }

    public Collection getUniqueFieldDefinitions() {
        Collection results = new ArrayList();
        for (Iterator i=fields.values().iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (!fieldDefinition.isUnique()) continue;
            results.add(fieldDefinition);
        }
        return results;
    }

    public Collection getIndexedFieldDefinitions() {
        Collection results = new ArrayList();
        for (Iterator i=fields.values().iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            if (!fieldDefinition.isPrimaryKey() && !fieldDefinition.isUnique() && !fieldDefinition.isIndex()) continue;
            results.add(fieldDefinition);
        }
        return results;
    }

	public Collection getFieldDefinitions() {
		return fields.values();
	}

	public void addFieldDefinition(FieldDefinition fieldDefinition) {
		fields.put(fieldDefinition.getName(), fieldDefinition);
	}

    public void renameFieldDefinition(String oldName, String newName) {
        if (oldName.equals(newName)) return;

        FieldDefinition fieldDefinition = (FieldDefinition)fields.get(oldName);
        if (fieldDefinition == null) return;

        fields.remove(oldName);
        fields.put(newName, fieldDefinition);
    }

    public void modifySourceDefinition(String name, FieldDefinition newFieldDefinition) {
        FieldDefinition fieldDefinition = (FieldDefinition)fields.get(name);
        fieldDefinition.copy(newFieldDefinition);
    }

    public void removeFieldDefinition(FieldDefinition fieldDefinition) {
        fields.remove(fieldDefinition.getName());
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

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (connectionName == null ? 0 : connectionName.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
                (fields == null ? 0 : fields.hashCode()) +
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

        SourceDefinition sourceDefinition = (SourceDefinition)object;
        if (!equals(name, sourceDefinition.name)) return false;
        if (!equals(connectionName, sourceDefinition.connectionName)) return false;
        if (!equals(description, sourceDefinition.description)) return false;
        if (!equals(fields, sourceDefinition.fields)) return false;
        if (!equals(parameters, sourceDefinition.parameters)) return false;

        return true;
    }

    public void copy(SourceDefinition sourceDefinition) {
        name = sourceDefinition.name;
        connectionName = sourceDefinition.connectionName;
        description = sourceDefinition.description;

        fields.clear();
        for (Iterator i = sourceDefinition.fields.values().iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)((FieldDefinition)i.next()).clone();
            addFieldDefinition((FieldDefinition)fieldDefinition.clone());
        }

        parameters.clear();
        parameters.putAll(sourceDefinition.parameters);
    }

    public Object clone() {
        SourceDefinition sourceDefinition = new SourceDefinition();
        sourceDefinition.copy(this);
        return sourceDefinition;
    }
}