/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connection.Adapter;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.AdapterConfig;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Source implements Cloneable, Serializable {

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

    private boolean includeOnAdd = true;
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

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (sourceName == null ? 0 : sourceName.hashCode()) +
                (connectionName == null ? 0 : connectionName.hashCode()) +
                (fields == null ? 0 : fields.hashCode()) +
                (includeOnAdd ? 0 : 1) +
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
        if (includeOnAdd != source.includeOnAdd) return false;
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

        source.includeOnAdd = includeOnAdd;
        source.includeOnDelete = includeOnDelete;

        return source;
    }

    public String toString() {
        return name+" "+sourceName;
    }

}