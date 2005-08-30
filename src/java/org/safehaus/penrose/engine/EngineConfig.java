/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import java.io.Serializable;
import java.util.Properties;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EngineConfig implements Serializable {

    private String engineName;
    private String engineClass;
    private String description;

    private Properties parameters = new Properties();

    public String getEngineClass() {
        return engineClass;
    }

    public void setEngineClass(String engineClass) {
        this.engineClass = engineClass;
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

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
