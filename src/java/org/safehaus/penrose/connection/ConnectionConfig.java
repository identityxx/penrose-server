/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.connection;

import org.safehaus.penrose.mapping.SourceDefinition;

import java.util.*;
import java.io.Serializable;


/**
 * @author Endi S. Dewata
 */
public class ConnectionConfig implements Serializable {

	/**
	 * Name.
	 */
	public String connectionName;

	/**
	 * Type.
	 */
	public String adapterName;

    private AdapterConfig adapterConfig;

	/**
	 * Connection pool size.
	 */
	public int poolSize;

	/**
	 * Connection pool test query.
	 */
	public String testQuery;

	/**
	 * Description
	 */
	public String description;

	/**
	 * Parameters.
	 */
	public Properties parameters = new Properties();

    private List listenerClasses = new ArrayList();
    private List listeners = new ArrayList();

    /**
     * Sources.
     */
    private Map sourceDefinitions = new TreeMap();

	/**
	 * Default constructor (provided for convenience for Apache Digester to be
	 * able to instantiate w/ default constructor)
	 */
	public ConnectionConfig() {
	}

	/**
	 * Constructor w/ name and type
	 * 
	 * @param name
	 *            the name of the connection
	 * @param type
	 *            the type of the connection, whether JNDI or LDAP
	 */
	public ConnectionConfig(String name, String type) {
		this.connectionName = name;
		this.adapterName = type;
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

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public void setParameter(String name, String value) {
        parameters.setProperty(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

	/**
	 * @return Returns the poolSize.
	 */
	public int getPoolSize() {
		return poolSize;
	}
	/**
	 * @param poolSize
	 *            The poolSize to set.
	 */
	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void setPoolSize(String poolSize) {
		this.poolSize = Integer.parseInt(poolSize);
	}
	/**
	 * @return Returns the testQuery.
	 */
	public String getTestQuery() {
		return testQuery;
	}
	/**
	 * @param testQuery
	 *            The testQuery to set.
	 */
	public void setTestQuery(String testQuery) {
		this.testQuery = testQuery;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description
	 *            the descripiton to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

    public List getListenerClasses() {
        return listenerClasses;
    }

    public void setListenerClasses(List listenerClasses) {
        this.listenerClasses = listenerClasses;
    }

    public List getListeners() {
        return listeners;
    }

    public void setListeners(List listeners) {
        this.listeners = listeners;
    }

    public void addListenerClass(String listener) {
        listenerClasses.add(listener);
    }

    public void removeListenerClass(String listener) {
        listenerClasses.remove(listener);
    }

    public void addListener(Object listener) {
        listeners.add(listener);
    }

    public void removeListener(Object listener) {
        listeners.remove(listener);
    }

    public AdapterConfig getAdapterConfig() {
        return adapterConfig;
    }

    public void setAdapterConfig(AdapterConfig adapterConfig) {
        this.adapterConfig = adapterConfig;
    }

    public int hashCode() {
        int value = connectionName.hashCode();
        //System.out.println("[ConnectionConfig("+connectionName+")] hashCode() => "+value);
        return value;
    }

    public boolean equals(Object object) {
        boolean value = false;
        try {
            if (object == null) {
                value = false;
                return value;
            }

            if (!(object instanceof ConnectionConfig)) {
                value = false;
                return value;
            }

            ConnectionConfig connectionConfig = (ConnectionConfig)object;
            if (!connectionName.equals(connectionConfig.connectionName)) {
                value = false;
                return value;
            }

            value = true;
            return value;

        } finally {
            //System.out.println("["+this+"] equals("+object+") => "+value);
        }
    }

    public Collection getSourceDefinitions() {
        return sourceDefinitions.values();
    }

    public SourceDefinition getSourceDefinition(String sourceName) {
        return (SourceDefinition)sourceDefinitions.get(sourceName);
    }

    public void addSourceDefinition(SourceDefinition sourceDefinition) {
        sourceDefinitions.put(sourceDefinition.getName(), sourceDefinition);
    }

    public SourceDefinition removeSourceDefinition(String sourceName) {
        return (SourceDefinition)sourceDefinitions.remove(sourceName);
    }

    public String toString() {
        return "ConnectionConfig("+connectionName+")";
    }
}