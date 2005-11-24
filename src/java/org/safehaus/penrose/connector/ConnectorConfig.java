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
package org.safehaus.penrose.connector;

import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.DefaultSourceCache;

import java.io.Serializable;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectorConfig implements Cloneable, Serializable {

    public final static String REFRESH_INTERVAL = "refreshInterval";
    public final static String THREAD_POOL_SIZE = "threadPoolSize";

    public final static String DEFAULT_CACHE_NAME  = "Source Cache";
    public final static String DEFAULT_CACHE_CLASS = DefaultSourceCache.class.getName();


    public final static int DEFAULT_REFRESH_INTERVAL = 60; // seconds
    public final static int DEFAULT_THREAD_POOL_SIZE = 20;

    public final static int DEFAULT_TIMEOUT          = 10000; // wait timeout is 10 seconds

    private String connectorName = "DEFAULT";
    private String connectorClass = Connector.class.getName();
    private String description;

    private Properties parameters = new Properties();

    public String getConnectorClass() {
        return connectorClass;
    }

    public void setConnectorClass(String connectorClass) {
        this.connectorClass = connectorClass;
    }

    public void setParameter(String name, String value) {
        parameters.setProperty(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int hashCode() {
        return (connectorName == null ? 0 : connectorName.hashCode()) +
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
        if (!equals(connectorName, connectorConfig.connectorName)) return false;
        if (!equals(connectorClass, connectorConfig.connectorClass)) return false;
        if (!equals(description, connectorConfig.description)) return false;
        if (!equals(parameters, connectorConfig.parameters)) return false;

        return true;
    }

    public Object clone() {
        ConnectorConfig connectorConfig = new ConnectorConfig();
        connectorConfig.copy(this);
        return connectorConfig;
    }

    public void copy(ConnectorConfig connectorConfig) {
        connectorName = connectorConfig.connectorName;
        connectorClass = connectorConfig.connectorClass;
        description = connectorConfig.description;

        parameters.clear();
        parameters.putAll(connectorConfig.parameters);
    }

}
