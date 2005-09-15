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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.module;import java.util.Collection;import org.safehaus.penrose.event.*;/** * @author Endi S. Dewata */public class GenericModule implements Module {    private ModuleConfig moduleConfig;    public ModuleConfig getModuleConfig() {        return moduleConfig;    }    public String getModuleName() {        return moduleConfig.getModuleName();    }        public String getModuleClass() {        return moduleConfig.getModuleClass();    }        public String getParameter(String name) {        return moduleConfig.getParameter(name);    }    public Collection getParameterNames() {        return moduleConfig.getParameterNames();    }    public void init(ModuleConfig moduleConfig) throws Exception {        this.moduleConfig = moduleConfig;        init();    }    public void init() throws Exception {    }    public void beforeBind(BindEvent event) throws Exception {    }    public void afterBind(BindEvent event) throws Exception {    }    public void beforeUnbind(BindEvent event) throws Exception {    }    public void afterUnbind(BindEvent event) throws Exception {    }    public void beforeAdd(AddEvent event) throws Exception {    }    public void afterAdd(AddEvent event) throws Exception {    }    public void beforeModify(ModifyEvent event) throws Exception {    }    public void afterModify(ModifyEvent event) throws Exception {    }    public void beforeDelete(DeleteEvent event) throws Exception {    }    public void afterDelete(DeleteEvent event) throws Exception {    }    public void beforeSearch(SearchEvent event) throws Exception {    }    public void afterSearch(SearchEvent event) throws Exception {    }}