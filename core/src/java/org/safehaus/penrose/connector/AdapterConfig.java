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

import java.util.Properties;
import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class AdapterConfig implements Cloneable, AdapterConfigMBean {

    private String name;
    private String adapterClass;
    private String description;

    private Properties parameters = new Properties();

    public AdapterConfig() {
    }

    public AdapterConfig(String adapterName, String adapterClass) {
        this.name = adapterName;
        this.adapterClass = adapterClass;
    }

    public String getAdapterClass() {
        return adapterClass;
    }

    public void setAdapterClass(String adapterClass) {
        this.adapterClass = adapterClass;
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

    public void setParameter(String name, String value) {
        parameters.setProperty(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public Map getParameters() {
        return parameters;
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (adapterClass == null ? 0 : adapterClass.hashCode()) +
                (description == null ? 0 : description.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if((object == null) || (object.getClass() != getClass())) return false;

        AdapterConfig adapterConfig = (AdapterConfig)object;
        if (!equals(name, adapterConfig.name)) return false;
        if (!equals(adapterClass, adapterConfig.adapterClass)) return false;
        if (!equals(description, adapterConfig.description)) return false;
        if (!equals(parameters, adapterConfig.parameters)) return false;

        return true;
    }

    public void copy(AdapterConfig adapterConfig) {
        name = adapterConfig.name;
        adapterClass = adapterConfig.adapterClass;
        description = adapterConfig.description;

        parameters.clear();
        parameters.putAll(adapterConfig.parameters);
    }

    public Object clone() {
        AdapterConfig adapterConfig = new AdapterConfig();
        adapterConfig.copy(this);
        return adapterConfig;
    }
}
