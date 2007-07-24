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
package org.safehaus.penrose.module;

import org.safehaus.penrose.event.*;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.session.SessionContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Module implements
        AddListener,
        BindListener,
        CompareListener,
        DeleteListener,
        ModifyListener,
        ModRdnListener,
        SearchListener,
        UnbindListener
{

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String STOPPING = "STOPPING";
    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";

    public PenroseConfig penroseConfig;
    public PenroseContext penroseContext;
    public SessionContext sessionContext;

    public Partition partition;
    public ModuleConfig moduleConfig;

    private String status = STOPPED;

    public String getName() {
        return moduleConfig.getName();
    }
    
    public String getParameter(String name) {
        return moduleConfig.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return moduleConfig.getParameterNames();
    }

    public void init() throws Exception {
    }

    public void start() throws Exception {
        setStatus(STARTED);
    }

    public void stop() throws Exception {
        setStatus(STOPPED);
    }

    public void beforeBind(BindEvent event) throws Exception {
    }

    public void afterBind(BindEvent event) throws Exception {
    }

    public void beforeUnbind(UnbindEvent event) throws Exception {
    }

    public void afterUnbind(UnbindEvent event) throws Exception {
    }

    public void beforeCompare(CompareEvent event) throws Exception {
    }

    public void afterCompare(CompareEvent event) throws Exception {
    }

    public void beforeAdd(AddEvent event) throws Exception {
    }

    public void afterAdd(AddEvent event) throws Exception {
    }

    public void beforeModify(ModifyEvent event) throws Exception {
    }

    public void afterModify(ModifyEvent event) throws Exception {
    }

    public void beforeModRdn(ModRdnEvent event) throws Exception {
    }

    public void afterModRdn(ModRdnEvent event) throws Exception {
    }

    public void beforeDelete(DeleteEvent event) throws Exception {
    }

    public void afterDelete(DeleteEvent event) throws Exception {
    }

    public void beforeSearch(SearchEvent event) throws Exception {
    }

    public void afterSearch(SearchEvent event) throws Exception {
    }

    public void setModuleConfig(ModuleConfig moduleConfig) {
        this.moduleConfig = moduleConfig;
    }

    public ModuleConfig getModuleConfig() {
        return moduleConfig;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }
}
