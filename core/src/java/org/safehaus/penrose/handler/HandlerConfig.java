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
package org.safehaus.penrose.handler;

import java.util.Properties;
import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class HandlerConfig implements HandlerConfigMBean, Cloneable {

    private String name = "DEFAULT";
    private String handlerClass = "org.safehaus.penrose.handler.Handler";
    private String description;

    private Properties parameters = new Properties();

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

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public Map getParameters() {
        return parameters;
    }

    public String getHandlerClass() {
        return handlerClass;
    }

    public void setHandlerClass(String handlerClass) {
        this.handlerClass = handlerClass;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (handlerClass == null ? 0 : handlerClass.hashCode()) +
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

        HandlerConfig handlerConfig = (HandlerConfig)object;
        if (!equals(name, handlerConfig.name)) return false;
        if (!equals(handlerClass, handlerConfig.handlerClass)) return false;
        if (!equals(description, handlerConfig.description)) return false;
        if (!equals(parameters, handlerConfig.parameters)) return false;

        return true;
    }

    public void copy(HandlerConfig handlerConfig) {
        name = handlerConfig.name;
        description = handlerConfig.description;
        handlerClass = handlerConfig.handlerClass;

        parameters.clear();
        parameters.putAll(handlerConfig.parameters);
    }

    public Object clone() {
        HandlerConfig sessionConfig = new HandlerConfig();
        sessionConfig.copy(this);
        return sessionConfig;
    }
}
