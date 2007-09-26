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
package org.safehaus.penrose.interpreter;

import java.util.Collection;
import java.util.Map;
import java.util.LinkedHashMap;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class InterpreterConfig implements Serializable, Cloneable {

    private String name = "DEFAULT";
    private String interpreterClass;
    private String description;

    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    public InterpreterConfig() {
    }

    public InterpreterConfig(String name, String interpreterClass) {
        this.name = name;
        this.interpreterClass = interpreterClass;
    }

    public String getInterpreterClass() {
        return interpreterClass;
    }

    public void setInterpreterClass(String interpreterClass) {
        this.interpreterClass = interpreterClass;
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public void setParameter(String name, String value) {
       parameters.put(name, value);
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
                (interpreterClass == null ? 0 : interpreterClass.hashCode()) +
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

        InterpreterConfig interpreterConfig = (InterpreterConfig)object;
        if (!equals(name, interpreterConfig.name)) return false;
        if (!equals(interpreterClass, interpreterConfig.interpreterClass)) return false;
        if (!equals(description, interpreterConfig.description)) return false;
        if (!equals(parameters, interpreterConfig.parameters)) return false;

        return true;
    }

    public void copy(InterpreterConfig interpreterConfig) {
        name = interpreterConfig.name;
        interpreterClass = interpreterConfig.interpreterClass;
        description = interpreterConfig.description;

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(interpreterConfig.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        InterpreterConfig interpreterConfig = (InterpreterConfig)super.clone();
        interpreterConfig.copy(this);
        return interpreterConfig;
    }
}
