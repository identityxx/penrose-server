package org.safehaus.penrose.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.event.EventManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.acl.ACLManager;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SessionContext {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private EngineManager  engineManager;
    private ACLManager     aclManager;

    private EventManager   eventManager;
    private ModuleManager  moduleManager;

    private HandlerManager handlerManager;
    private SessionManager sessionManager;

    public SessionContext() {
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

    public EngineManager getEngineManager() {
        return engineManager;
    }

    public void setEngineManager(EngineManager engineManager) {
        this.engineManager = engineManager;
    }

    public ACLManager getAclManager() {
        return aclManager;
    }

    public void setAclManager(ACLManager aclManager) {
        this.aclManager = aclManager;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public EventManager getEventManager() {
        return eventManager;
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }

    public HandlerManager getHandlerManager() {
        return handlerManager;
    }

    public void setHandlerManager(HandlerManager handlerManager) {
        this.handlerManager = handlerManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public void init() throws Exception {

        engineManager = new EngineManager();
        engineManager.setPenroseConfig(penroseConfig);
        engineManager.setPenroseContext(penroseContext);
        engineManager.setSessionContext(this);

        aclManager = new ACLManager();
        aclManager.setPenroseConfig(penroseConfig);
        aclManager.setPenroseContext(penroseContext);

        moduleManager = new ModuleManager();
        moduleManager.setPenroseConfig(penroseConfig);
        moduleManager.setPenroseContext(penroseContext);
        moduleManager.setSessionContext(this);

        eventManager = new EventManager();
        eventManager.setPenroseConfig(penroseConfig);
        eventManager.setPenroseContext(penroseContext);
        eventManager.setSessionContext(this);

        handlerManager = new HandlerManager();
        handlerManager.setPenroseConfig(penroseConfig);
        handlerManager.setPenroseContext(penroseContext);
        handlerManager.setSessionContext(this);

        sessionManager = new SessionManager();
        sessionManager.setPenroseConfig(penroseConfig);
        sessionManager.setPenroseContext(penroseContext);
        sessionManager.setSessionContext(this);
    }

    public void load() throws Exception {

        for (Iterator i=penroseConfig.getEngineConfigs().iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();
            engineManager.init(engineConfig);
        }

        for (Iterator i=penroseConfig.getHandlerConfigs().iterator(); i.hasNext(); ) {
            HandlerConfig handlerConfig = (HandlerConfig)i.next();
            handlerManager.init(handlerConfig);
        }

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            for (Iterator j=partition.getModules().getModuleConfigs().iterator(); j.hasNext(); ) {
                ModuleConfig moduleConfig = (ModuleConfig)j.next();
                moduleManager.init(partition, moduleConfig);
            }
        }
    }

    public void start() throws Exception {
        engineManager.start();
        moduleManager.start();
        handlerManager.start();
        sessionManager.start();
    }

    public void stop() throws Exception {
        sessionManager.stop();
        handlerManager.stop();
        moduleManager.stop();
        engineManager.stop();
    }

    public void clear() throws Exception {
        handlerManager.clear();
        engineManager.clear();
    }
}
