/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;


import java.util.Collection;
import java.util.Map;

import org.safehaus.penrose.event.*;

/**
 * @author Endi S. Dewata
 */
public class GenericModule implements Module, ModuleConfig {

    private ModuleConfig moduleConfig;

    public ModuleConfig getModuleConfig() {
        return moduleConfig;
    }

    public String getModuleName() {
        return moduleConfig.getModuleName();
    }
    
    public String getModuleClass() {
        return moduleConfig.getModuleClass();
    }
    
    public String getParameter(String name) {
        return moduleConfig.getParameter(name);
    }

    public Collection getParameterNames() {
        return moduleConfig.getParameterNames();
    }

    public void init(ModuleConfig moduleConfig) throws Exception {
        this.moduleConfig = moduleConfig;

        init();
    }

    public void init() throws Exception {

    }

    public void beforeBind(BindEvent event) throws Exception {
    }

    public void afterBind(BindEvent event) throws Exception {
    }

    public void beforeUnbind(BindEvent event) throws Exception {
    }

    public void afterUnbind(BindEvent event) throws Exception {
    }

    public void beforeAdd(AddEvent event) throws Exception {
    }

    public void afterAdd(AddEvent event) throws Exception {
    }

    public void beforeModify(ModifyEvent event) throws Exception {
    }

    public void afterModify(ModifyEvent event) throws Exception {
    }

    public void beforeDelete(DeleteEvent event) throws Exception {
    }

    public void afterDelete(DeleteEvent event) throws Exception {
    }

    public void beforeSearch(SearchEvent event) throws Exception {
    }

    public void afterSearch(SearchEvent event) throws Exception {
    }
}
