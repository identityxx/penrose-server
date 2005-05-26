/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class GenericModuleConfig implements ModuleConfig {

    public String moduleName;
    public String moduleClass;
    public LinkedHashMap parameters = new LinkedHashMap();

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public void clearParameters() {
        parameters.clear();
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String getParameter(String name) {
        return (String)parameters.get(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public String getModuleClass() {
        return moduleClass;
    }

    public void setModuleClass(String moduleClass) {
        this.moduleClass = moduleClass;
    }
}
