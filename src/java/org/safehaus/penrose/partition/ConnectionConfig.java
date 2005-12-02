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
package org.safehaus.penrose.partition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionConfig implements Cloneable {

	/**
	 * Name.
	 */
	public String name;

	/**
	 * Type.
	 */
	public String adapterName;

	/**
	 * Description
	 */
	public String description;

	/**
	 * Parameters.
	 */
	public Map parameters = new TreeMap();

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
		this.name = name;
		this.adapterName = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
        return (String)parameters.get(name);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
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

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (adapterName == null ? 0 : adapterName.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
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

        ConnectionConfig connectionConfig = (ConnectionConfig)object;
        if (!equals(name, connectionConfig.name)) return false;
        if (!equals(adapterName, connectionConfig.adapterName)) return false;
        if (!equals(description, connectionConfig.description)) return false;
        if (!equals(parameters, connectionConfig.parameters)) return false;

        return true;
    }

    public String toString() {
        return "ConnectionConfig("+name+")";
    }

    public void copy(ConnectionConfig connectionConfig) {
        name = connectionConfig.name;
        adapterName = connectionConfig.adapterName;
        description = connectionConfig.description;
        parameters.putAll(connectionConfig.parameters);
    }

    public Object clone() {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.copy(this);

        return connectionConfig;
    }
}