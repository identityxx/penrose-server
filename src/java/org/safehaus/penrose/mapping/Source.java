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

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.connector.AdapterConfig;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Source implements Cloneable, Serializable {

    public final static String FILTER     = "filter";

	/**
	 * Name.
	 */
	private String name;

    /**
     * Source name.
     */
    private String sourceName;

    /**
     * Connection name.
     */
    private String connectionName;

	/**
	 * Fields. Each element is of type org.safehaus.penrose.mapping.Field.
	 */
	private Map fields = new TreeMap();

    /**
     * Parameters.
     */
    private Properties parameters = new Properties();

    private boolean required = true;
    private boolean readOnly = false;

    private boolean includeOnAdd = true;
    private boolean includeOnModify = true;
    private boolean includeOnModRdn = true;
    private boolean includeOnDelete = true;

	public Source() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public Field getField(String name) {
        return (Field)fields.get(name);
    }

	public Collection getFields() {
		return fields.values();
	}

	public void addField(Field field) {
		fields.put(field.getName(), field);
	}

    public Field removeField(String name) {
        return (Field)fields.remove(name);
    }

    public void removeFields() {
        fields.clear();
    }
    
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
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

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public boolean isIncludeOnDelete() {
        return includeOnDelete;
    }

    public void setIncludeOnDelete(boolean includeOnDelete) {
        this.includeOnDelete = includeOnDelete;
    }

    public boolean isIncludeOnAdd() {
        return includeOnAdd;
    }

    public void setIncludeOnAdd(boolean includeOnAdd) {
        this.includeOnAdd = includeOnAdd;
    }

    public boolean isIncludeOnModify() {
        return includeOnModify;
    }

    public void setIncludeOnModify(boolean includeOnModify) {
        this.includeOnModify = includeOnModify;
    }

    public boolean isIncludeOnModRdn() {
        return includeOnModRdn;
    }

    public void setIncludeOnModRdn(boolean includeOnModRdn) {
        this.includeOnModRdn = includeOnModRdn;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (sourceName == null ? 0 : sourceName.hashCode()) +
                (connectionName == null ? 0 : connectionName.hashCode()) +
                (fields == null ? 0 : fields.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode()) +
                (required ? 0 : 1) +
                (readOnly ? 0 : 1) +
                (includeOnAdd ? 0 : 1) +
                (includeOnModify? 0 : 1) +
                (includeOnModRdn? 0 : 1) +
                (includeOnDelete ? 0 : 1);
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        Source source = (Source)object;
        if (!equals(name, source.name)) return false;
        if (!equals(sourceName, source.sourceName)) return false;
        if (!equals(connectionName, source.connectionName)) return false;
        if (!equals(fields, source.fields)) return false;
        if (!equals(parameters, source.parameters)) return false;
        if (required != source.required) return false;
        if (readOnly != source.readOnly) return false;
        if (includeOnAdd != source.includeOnAdd) return false;
        if (includeOnModify != source.includeOnModify) return false;
        if (includeOnModRdn != source.includeOnModRdn) return false;
        if (includeOnDelete != source.includeOnDelete) return false;

        return true;
    }

    public Object clone() {
        Source source = new Source();
        source.name = name;
        source.sourceName = sourceName;
        source.connectionName = connectionName;

        for (Iterator i=fields.values().iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            source.addField((Field)field.clone());
        }

        source.parameters.putAll(source.parameters);

        source.required = required;
        source.readOnly = readOnly;
        source.includeOnAdd = includeOnAdd;
        source.includeOnModify = includeOnModify;
        source.includeOnModRdn = includeOnModRdn;
        source.includeOnDelete = includeOnDelete;

        return source;
    }

    public String toString() {
        return name+" "+sourceName;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }
}