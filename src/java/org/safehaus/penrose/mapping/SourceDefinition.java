/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import org.safehaus.penrose.connection.AdapterConfig;
import org.safehaus.penrose.connection.ConnectionConfig;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceDefinition {

    public final static String LOAD_ON_STARTUP      = "loadOnStartup";
    public final static String CACHE_EXPIRATION     = "cacheExpiration";
    public final static String LOAD_UPON_EXPIRATION = "loadUponExpiration";

	/**
	 * Name.
	 */
	private String name;

	/**
	 * Connection name.
	 */
	private String connectionName;
    
    private ConnectionConfig connectionConfig;
    private AdapterConfig adapterConfig;

	/**
	 * Fields. Each element is of type org.safehaus.penrose.mapping.FieldDefinition.
	 */
	private Map fields = new TreeMap();

    /**
     * Fields. Each element is of type org.safehaus.penrose.mapping.FieldDefinition.
     */
    private List primaryKeyFields = new ArrayList();

    /**
     * Parameters.
     */
    private Properties parameters = new Properties();

    private List listeners = new ArrayList();

    private String description;

	public SourceDefinition() {
	}
	
	public SourceDefinition(String name, String connection) {
		this.name = name;
		this.connectionName = connection;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getConnectionName() {
		return connectionName;
	}

	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}

    public FieldDefinition getFieldDefinition(String name) {
        return (FieldDefinition)fields.get(name);
    }
    
	public Collection getFields() {
		return fields.values();
	}

    public Collection getPrimaryKeyFields() {
        return primaryKeyFields;
    }

	public void addFieldDefinition(FieldDefinition fieldConfig) {
		fields.put(fieldConfig.getName(), fieldConfig);
        if (fieldConfig.isPrimaryKey()) primaryKeyFields.add(fieldConfig);
	}

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }
    public List getListeners() {
        return listeners;
    }

    public void setListeners(List listeners) {
        this.listeners = listeners;
    }

    public void addListener(Object listener) {
        listeners.add(listener);
    }

    public void removeListener(Object listener) {
        listeners.remove(listener);
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
    }
}