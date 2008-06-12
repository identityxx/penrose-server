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

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.module.ModuleChain;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Module {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean warn = log.isWarnEnabled();
    public boolean debug = log.isDebugEnabled();

    public ModuleConfig moduleConfig;
    public ModuleContext moduleContext;

    public Partition partition;

    public String getName() {
        return moduleConfig.getName();
    }

    public String getDescription() {
        return moduleConfig.getDescription();
    }
    
    public String getParameter(String name) {
        return moduleConfig.getParameter(name);
    }

    public Collection<String> getParameterNames() {
        return moduleConfig.getParameterNames();
    }

    public boolean isEnabled() {
        return moduleConfig.isEnabled();
    }

    public void init(ModuleConfig moduleConfig, ModuleContext moduleContext) throws Exception {

        if (debug) log.debug("Initializing module "+moduleConfig.getName()+".");

        this.moduleConfig = moduleConfig;
        this.moduleContext = moduleContext;

        partition = moduleContext.getPartition();

        init();
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.add(session, request, response);
    }

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.bind(session, request, response);
    }

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.compare(session, request, response);
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.delete(session, request, response);
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.modify(session, request, response);
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.modrdn(session, request, response);
    }

    public void search(
            Session session,
            SearchRequest request,
            SearchResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.search(session, request, response);
    }

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response,
            ModuleChain chain
    ) throws Exception {
        chain.unbind(session, request, response);
    }

    public void setModuleConfig(ModuleConfig moduleConfig) {
        this.moduleConfig = moduleConfig;
    }

    public ModuleConfig getModuleConfig() {
        return moduleConfig;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public ModuleContext getModuleContext() {
        return moduleContext;
    }

    public void setModuleContext(ModuleContext moduleContext) {
        this.moduleContext = moduleContext;
    }

    public Session createAdminSession() throws Exception {
        SessionManager sessionManager = getPartition().getPartitionContext().getSessionManager();
        return sessionManager.createAdminSession();
    }
}
