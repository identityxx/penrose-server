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
package org.safehaus.penrose.connection;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionConfig implements ConnectionConfigMBean, Cloneable {

    private boolean enabled = true;

	public String name;
    public String description;

	public String adapterName;

	public Map<String,String> parameters = new LinkedHashMap<String,String>();

	public ConnectionConfig() {
	}

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

    public Map<String,String> getParameters() {
        return parameters;
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameters(Map<String,String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }
    
    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String removeParameter(String name) {
        return parameters.remove(name);
    }

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        ConnectionConfig connectionConfig = (ConnectionConfig)object;
        if (enabled != connectionConfig.enabled) return false;

        if (!equals(name, connectionConfig.name)) return false;
        if (!equals(description, connectionConfig.description)) return false;

        if (!equals(adapterName, connectionConfig.adapterName)) return false;

        if (!equals(parameters, connectionConfig.parameters)) return false;

        return true;
    }

    public void copy(ConnectionConfig connectionConfig) {
        enabled = connectionConfig.enabled;

        name = connectionConfig.name;
        description = connectionConfig.description;

        adapterName = connectionConfig.adapterName;

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(connectionConfig.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        ConnectionConfig connectionConfig = (ConnectionConfig)super.clone();
        connectionConfig.copy(this);
        return connectionConfig;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}