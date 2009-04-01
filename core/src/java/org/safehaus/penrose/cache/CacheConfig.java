/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.cache;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Administrator
 */
public class CacheConfig implements Cloneable {
	
    private String name;
    private String cacheClass;
    private String description;

    public final static String CACHE_SIZE              = "cacheSize";
    public final static String CACHE_EXPIRATION        = "cacheExpiration";

    public final static int DEFAULT_CACHE_SIZE         = 100;
    public final static int DEFAULT_CACHE_EXPIRATION   = 5;

	/**
	 * Parameters.
	 */
	public Map parameters = new TreeMap();

	public Collection getParameterNames() {
		return parameters.keySet();
	}

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public String getCacheClass() {
        return cacheClass;
    }

    public void setCacheClass(String cacheClass) {
        this.cacheClass = cacheClass;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

        CacheConfig cacheConfig = (CacheConfig)object;
        if (!equals(name, cacheConfig.name)) return false;
        if (!equals(cacheClass, cacheConfig.cacheClass)) return false;
        if (!equals(description, cacheConfig.description)) return false;
        if (!equals(parameters, cacheConfig.parameters)) return false;

        return true;
    }

    public void copy(CacheConfig cacheConfig) {
        name = cacheConfig.name;
        cacheClass = cacheConfig.cacheClass;
        description = cacheConfig.description;

        parameters = new TreeMap();
        parameters.putAll(cacheConfig.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        CacheConfig cacheConfig = (CacheConfig)super.clone();
        cacheConfig.copy(this);
        return cacheConfig;
    }
}