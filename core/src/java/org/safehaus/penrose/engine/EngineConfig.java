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
package org.safehaus.penrose.engine;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EngineConfig implements Cloneable {

    public final static String ALLOW_CONCURRENCY   = "allowConcurrency";

    public final static String THREAD_POOL_SIZE    = "threadPoolSize";


    public final static int DEFAULT_THREAD_POOL_SIZE = 20;

    private String name = "DEFAULT";
    private String engineClass = "org.safehaus.penrose.engine.DefaultEngine";
    private String description;

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public EngineConfig() {
    }

    public EngineConfig(String name, String engineClass) {
        this.name = name;
        this.engineClass = engineClass;
    }

    public String getEngineClass() {
        return engineClass;
    }

    public void setEngineClass(String engineClass) {
        this.engineClass = engineClass;
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
                (engineClass == null ? 0 : engineClass.hashCode()) +
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
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        EngineConfig engineConfig = (EngineConfig)object;
        if (!equals(name, engineConfig.name)) return false;
        if (!equals(engineClass, engineConfig.engineClass)) return false;
        if (!equals(description, engineConfig.description)) return false;
        if (!equals(parameters, engineConfig.parameters)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        EngineConfig engineConfig = (EngineConfig)super.clone();
        engineConfig.copy(this);
        return engineConfig;
    }

    public void copy(EngineConfig engineConfig) {
        name = engineConfig.name;
        engineClass = engineConfig.engineClass;
        description = engineConfig.description;

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(engineConfig.parameters);
    }
}
