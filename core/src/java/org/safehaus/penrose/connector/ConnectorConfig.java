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
package org.safehaus.penrose.connector;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectorConfig implements Cloneable {

    public final static String ALLOW_CONCURRENCY   = "allowConcurrency";

    public final static String REFRESH_INTERVAL    = "refreshInterval";
    public final static String THREAD_POOL_SIZE    = "threadPoolSize";

    public final static String DEFAULT_CACHE_NAME  = "Source Cache";
    public final static String DEFAULT_CACHE_CLASS = null;


    public final static int DEFAULT_REFRESH_INTERVAL = 60; // seconds
    public final static int DEFAULT_THREAD_POOL_SIZE = 20;

    public final static int DEFAULT_TIMEOUT          = 10000; // wait timeout is 10 seconds

    private String name = "DEFAULT";
    private String connectorClass = Connector.class.getName();
    private String description;

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public String getConnectorClass() {
        return connectorClass;
    }

    public void setConnectorClass(String connectorClass) {
        this.connectorClass = connectorClass;
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (connectorClass == null ? 0 : connectorClass.hashCode()) +
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

        ConnectorConfig connectorConfig = (ConnectorConfig)object;
        if (!equals(name, connectorConfig.name)) return false;
        if (!equals(connectorClass, connectorConfig.connectorClass)) return false;
        if (!equals(description, connectorConfig.description)) return false;
        if (!equals(parameters, connectorConfig.parameters)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        super.clone();
        ConnectorConfig connectorConfig = new ConnectorConfig();
        connectorConfig.copy(this);
        return connectorConfig;
    }

    public void copy(ConnectorConfig connectorConfig) {
        name = connectorConfig.name;
        connectorClass = connectorConfig.connectorClass;
        description = connectorConfig.description;

        parameters.clear();
        parameters.putAll(connectorConfig.parameters);
    }

}
