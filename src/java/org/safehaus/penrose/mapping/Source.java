/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
    private Collection primaryKeyFields = new ArrayList();

    private SourceDefinition sourceDefinition;
    private ConnectionConfig connectionConfig;

	public Source() {
	}
	
    public Collection getPrimaryKeyFields() {
        return primaryKeyFields;
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

    public void removeAllFields() {
        fields.clear();
    }
    
    public void addPrimaryKeyField(Field field) {
        primaryKeyFields.add(field);
    }

    /**
     * Clone this object
     */
    public Object clone() {
        Source source = new Source();
        source.setName(name);

        for (Iterator i=fields.values().iterator(); i.hasNext(); ) {
            Field field = (Field)i.next();
            source.addField(field);
        }

        return source;
    }
    
    public SourceDefinition getSourceDefinition() {
        return sourceDefinition;
    }

    public String getParameter(String key) {
        return sourceDefinition.getParameter(key);
    }

    public Collection getParameterNames() {
        return sourceDefinition.getParameterNames();
    }

    public void setSourceDefinition(SourceDefinition sourceDefinition) {
        this.sourceDefinition = sourceDefinition;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String toString() {
        return name+" "+sourceName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }
}