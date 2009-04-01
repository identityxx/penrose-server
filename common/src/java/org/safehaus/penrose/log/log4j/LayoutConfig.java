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
package org.safehaus.penrose.log.log4j;

import java.util.Collection;
import java.util.Map;
import java.util.LinkedHashMap;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class LayoutConfig implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    String layoutClass;
    Map<String,String> parameters = new LinkedHashMap<String,String>();

    public Map<String,String> getParameters() {
        return parameters;
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void setParameters(Map<String,String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        if (parameters == null) return;
        this.parameters.putAll(parameters);
    }

    public String removeParameter(String name) {
        return parameters.remove(name);
    }

    public void removeParameters() {
        parameters.clear();
    }

    public String getLayoutClass() {
        return layoutClass;
    }

    public void setLayoutClass(String layoutClass) {
        this.layoutClass = layoutClass;
    }

    public int hashCode() {
        return (layoutClass == null ? 0 : layoutClass.hashCode())
                + parameters.hashCode();
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

        LayoutConfig layoutConfig = (LayoutConfig)object;
        if (!equals(layoutClass, layoutConfig.layoutClass)) return false;
        if (!equals(parameters, layoutConfig.parameters)) return false;

        return true;
    }

    public void copy(LayoutConfig layoutConfig) throws CloneNotSupportedException {
        layoutClass = layoutConfig.layoutClass;

        parameters = new LinkedHashMap<String,String>();
        parameters.putAll(layoutConfig.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        LayoutConfig layoutConfig = (LayoutConfig)super.clone();
        layoutConfig.copy(this);
        return layoutConfig;
    }
}
