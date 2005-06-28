/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connection.Adapter;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionConfig;
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
	 * Fields. Each element is of type org.safehaus.penrose.mapping.Field.
	 */
	private Map fields = new TreeMap();
    private Collection primaryKeyFields = new ArrayList();

    private SourceDefinition sourceDefinition;
    private Connection connection;
    private Adapter adapter;

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
        sourceName = name;
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

    public int bind(AttributeValues values, String password) throws Exception {
        return adapter.bind(this, values, password);
    }

    public SearchResults search(Filter filter) throws Exception {
        return adapter.search(this, filter);
    }

    public int add(AttributeValues values) throws Exception {
        return adapter.add(this, values);
    }

    public int modify(AttributeValues oldValues, AttributeValues newValues) throws Exception {
        return adapter.modify(this, oldValues, newValues);
    }

    public int delete(AttributeValues values) throws Exception {
        return adapter.delete(this, values);
    }

    public void setSourceDefinition(SourceDefinition sourceDefinition) {
        this.sourceDefinition = sourceDefinition;
    }

    public Adapter getAdapter() {
        return adapter;
    }

    public void setAdapter(Adapter adapter) {
        this.adapter = adapter;
    }

    public AdapterConfig getAdapterConfig() {
        return sourceDefinition.getAdapterConfig();
    }

    public String getConnectionName() {
        return sourceDefinition.getConnectionName();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConnectionConfig getConnectionConfig() {
        return sourceDefinition.getConnectionConfig();
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
}