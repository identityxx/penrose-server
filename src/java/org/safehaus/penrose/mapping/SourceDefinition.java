/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import org.safehaus.penrose.connection.AdapterConfig;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceDefinition implements Cloneable {

    public final static String LOAD_ON_STARTUP      = "loadOnStartup";
    public final static String CACHE_EXPIRATION     = "cacheExpiration";
    public final static String LOAD_UPON_EXPIRATION = "loadUponExpiration";

	/**
	 * Name.
	 */
	private String name;

    private String description;

    private ConnectionConfig connectionConfig;

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

    /**
     * Listeners.
     */
    private List listeners = new ArrayList();

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

    public void removeParameter(String name) {
        parameters.remove(name);
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

    public Object clone() {
        SourceDefinition sourceDefinition = new SourceDefinition();
        sourceDefinition.name = name;
        sourceDefinition.description = description;
        sourceDefinition.connectionConfig = connectionConfig;

        for (Iterator i = fields.values().iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)((FieldDefinition)i.next()).clone();
            sourceDefinition.addFieldDefinition(fieldDefinition);
        }

        sourceDefinition.parameters.putAll(parameters);
        sourceDefinition.listeners.addAll(listeners);

        return sourceDefinition;
    }
}