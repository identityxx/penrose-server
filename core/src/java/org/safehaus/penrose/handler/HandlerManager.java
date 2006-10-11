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

import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.thread.ThreadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.Map;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class HandlerManager {

    Logger log = LoggerFactory.getLogger(getClass());
    
    Map handlers = new TreeMap();

    private Penrose penrose;
    private SessionManager sessionManager;
    private SchemaManager schemaManager;
    private InterpreterManager interpreterManager;

    private PenroseConfig penroseConfig;
    private PartitionManager partitionManager;
    private ModuleManager moduleManager;
    private ThreadManager threadManager;

    public HandlerManager() {
    }

    public void init(HandlerConfig handlerConfig, EngineManager engineManager) throws Exception {

        log.debug("Initializing handler.");

        Handler handler = new Handler(penrose);
        handler.setSessionManager(sessionManager);
        handler.setSchemaManager(schemaManager);
        handler.setInterpreterFactory(interpreterManager);
        handler.setEngineManager(engineManager);
        handler.setPartitionManager(partitionManager);
        handler.setThreadManager(threadManager);

        handlers.put(handlerConfig.getName(), handler);
    }

    public Handler getHandler(String name) {
        return (Handler)handlers.get(name);
    }

    public void clear() {
        handlers.clear();
    }

    public void start() throws Exception {
        for (Iterator i=handlers.values().iterator(); i.hasNext(); ) {
            Handler handler = (Handler)i.next();
            handler.start();
        }
    }

    public void stop() throws Exception {
        for (Iterator i=handlers.values().iterator(); i.hasNext(); ) {
            Handler handler = (Handler)i.next();
            handler.stop();
        }
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public InterpreterManager getInterpreterFactory() {
        return interpreterManager;
    }

    public void setInterpreterFactory(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public Penrose getPenrose() {
        return penrose;
    }

    public void setPenrose(Penrose penrose) {
        this.penrose = penrose;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }
}
