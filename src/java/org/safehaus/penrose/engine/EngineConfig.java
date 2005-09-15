/** * Copyright (c) 2000-2005, Identyx Corporation.
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.engine;import java.io.Serializable;import java.util.Properties;import java.util.Collection;/** * @author Endi S. Dewata */public class EngineConfig implements Serializable {    private String engineName;    private String engineClass;    private String description;    private Properties parameters = new Properties();    public String getEngineClass() {        return engineClass;    }    public void setEngineClass(String engineClass) {        this.engineClass = engineClass;    }    public void setParameter(String name, String value) {        parameters.setProperty(name, value);    }    public void removeParameter(String name) {        parameters.remove(name);    }    public Collection getParameterNames() {        return parameters.keySet();    }    public String getParameter(String name) {        return parameters.getProperty(name);    }    public String getEngineName() {        return engineName;    }    public void setEngineName(String engineName) {        this.engineName = engineName;    }    public String getDescription() {        return description;    }    public void setDescription(String description) {        this.description = description;    }}