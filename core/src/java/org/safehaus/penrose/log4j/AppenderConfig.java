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
package org.safehaus.penrose.log4j;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class AppenderConfig implements Cloneable, Serializable {

    private String name;
    private String appenderClass;

    LayoutConfig layoutConfig;

    Map parameters = new LinkedHashMap();

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String removeParameter(String name) {
        return (String)parameters.remove(name);
    }

    public void clearParameters() {
        parameters.clear();
    }

    public LayoutConfig getLayoutConfig() {
        return layoutConfig;
    }

    public void setLayoutConfig(LayoutConfig layoutConfig) {
        this.layoutConfig = layoutConfig;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAppenderClass() {
        return appenderClass;
    }

    public void setAppenderClass(String appenderClass) {
        this.appenderClass = appenderClass;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (appenderClass == null ? 0 : appenderClass.hashCode()) +
                (layoutConfig == null ? 0 : layoutConfig.hashCode()) +
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

        AppenderConfig appenderConfig = (AppenderConfig)object;
        if (!equals(name, appenderConfig.name)) return false;
        if (!equals(appenderClass, appenderConfig.appenderClass)) return false;
        if (!equals(layoutConfig, appenderConfig.layoutConfig)) return false;
        if (!equals(parameters, appenderConfig.parameters)) return false;

        return true;
    }

    public void copy(AppenderConfig appenderConfig) {
        name = appenderConfig.name;
        appenderClass = appenderConfig.appenderClass;
        layoutConfig = (LayoutConfig)appenderConfig.layoutConfig.clone();

        parameters.clear();
        parameters.putAll(appenderConfig.parameters);
    }

    public Object clone() {
        AppenderConfig appenderConfig = new AppenderConfig();
        appenderConfig.copy(this);
        return appenderConfig;
    }
}
