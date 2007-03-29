package org.safehaus.penrose.naming;

import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connector.ConnectorManager;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.event.EventManager;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.PenroseConfigWriter;
import org.safehaus.penrose.log4j.Log4jConfigReader;
import org.safehaus.penrose.log4j.Log4jConfig;
import org.safehaus.penrose.log4j.AppenderConfig;
import org.safehaus.penrose.log4j.LoggerConfig;
import org.safehaus.penrose.acl.ACLManager;
import org.safehaus.penrose.source.SourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenroseContext {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String THREAD_MANAGER      = "java:comp/org/safehaus/penrose/thread/ThreadManager";
    public final static String SCHEMA_MANAGER      = "java:comp/org/safehaus/penrose/schema/SchemaManager";

    public final static String SESSION_MANAGER     = "java:comp/org/safehaus/penrose/session/SessionManager";
    public final static String HANDLER_MANAGER     = "java:comp/org/safehaus/penrose/handler/HandlerManager";
    public final static String EVENT_MANAGER       = "java:comp/org/safehaus/penrose/event/EventManager";

    public final static String ENGINE_MANAGER      = "java:comp/org/safehaus/penrose/engine/EngineManager";
    public final static String INTERPRETER_MANAGER = "java:comp/org/safehaus/penrose/interpreter/InterpreterManager";
    public final static String CONNECTOR_MANAGER   = "java:comp/org/safehaus/penrose/connector/ConnectorManager";

    public final static String PARTITION_MANAGER   = "java:comp/org/safehaus/penrose/partition/PartitionManager";
    public final static String CONNECTION_MANAGER  = "java:comp/org/safehaus/penrose/connection/ConnectionManager";
    public final static String SOURCE_MANAGER      = "java:comp/org/safehaus/penrose/source/SourceManager";
    public final static String MODULE_MANAGER      = "java:comp/org/safehaus/penrose/module/ModuleManager";

    private PenroseConfig      penroseConfig;

    private ThreadManager      threadManager;

    private SchemaManager      schemaManager;

    private InterpreterManager interpreterManager;
    private ConnectorManager   connectorManager;
    private EngineManager      engineManager;
    private ACLManager         aclManager;

    private SessionManager     sessionManager;
    private HandlerManager     handlerManager;
    private EventManager       eventManager;

    private PartitionManager   partitionManager;
    private ConnectionManager  connectionManager;
    private SourceManager      sourceManager;
    private ModuleManager      moduleManager;

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
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

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public HandlerManager getHandlerManager() {
        return handlerManager;
    }

    public void setHandlerManager(HandlerManager handlerManager) {
        this.handlerManager = handlerManager;
    }

    public EventManager getEventManager() {
        return eventManager;
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }

    public EngineManager getEngineManager() {
        return engineManager;
    }

    public void setEngineManager(EngineManager engineManager) {
        this.engineManager = engineManager;
    }

    public InterpreterManager getInterpreterManager() {
        return interpreterManager;
    }

    public void setInterpreterManager(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public ConnectorManager getConnectorManager() {
        return connectorManager;
    }

    public void setConnectorManager(ConnectorManager connectorManager) {
        this.connectorManager = connectorManager;
    }

    public ACLManager getAclManager() {
        return aclManager;
    }

    public void setAclManager(ACLManager aclManager) {
        this.aclManager = aclManager;
    }

    public SourceManager getSourceManager() {
        return sourceManager;
    }

    public void setSourceManager(SourceManager sourceManager) {
        this.sourceManager = sourceManager;
    }

    public void init(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;

        String home = penroseConfig.getHome();

        File log4jXml = new File((home == null ? "" : home+File.separator)+"conf"+File.separator+"log4j.xml");
        if (!log4jXml.exists()) return;

        Log4jConfigReader configReader = new Log4jConfigReader(log4jXml);
        Log4jConfig config = configReader.read();

        log.debug("Appenders:");
        for (Iterator i=config.getAppenderConfigs().iterator(); i.hasNext(); ) {
            AppenderConfig appenderConfig = (AppenderConfig)i.next();
            log.debug(" - "+appenderConfig.getName());
        }

        log.debug("Loggers:");
        for (Iterator i=config.getLoggerConfigs().iterator(); i.hasNext(); ) {
            LoggerConfig loggerConfig = (LoggerConfig)i.next();
            log.debug(" - "+loggerConfig.getName()+": "+loggerConfig.getLevel()+" "+loggerConfig.getAppenders());
        }

        threadManager = new ThreadManager();
        threadManager.setPenroseConfig(penroseConfig);
        threadManager.setPenroseContext(this);

        schemaManager = new SchemaManager();
        schemaManager.setPenroseConfig(penroseConfig);
        schemaManager.setPenroseContext(this);

        interpreterManager = new InterpreterManager();
        interpreterManager.setPenroseConfig(penroseConfig);
        interpreterManager.setPenroseContext(this);

        connectorManager = new ConnectorManager();
        connectorManager.setPenroseConfig(penroseConfig);
        connectorManager.setPenroseContext(this);

        engineManager = new EngineManager();
        engineManager.setPenroseConfig(penroseConfig);
        engineManager.setPenroseContext(this);

        aclManager = new ACLManager();
        aclManager.setPenroseConfig(penroseConfig);
        aclManager.setPenroseContext(this);

        sessionManager = new SessionManager();
        sessionManager.setPenroseConfig(penroseConfig);
        sessionManager.setPenroseContext(this);

        eventManager = new EventManager();
        eventManager.setPenroseConfig(penroseConfig);
        eventManager.setPenroseContext(this);

        handlerManager = new HandlerManager();
        handlerManager.setPenroseConfig(penroseConfig);
        handlerManager.setPenroseContext(this);

        partitionManager = new PartitionManager();
        partitionManager.setPenroseConfig(penroseConfig);
        partitionManager.setPenroseContext(this);

        connectionManager = new ConnectionManager();
        connectionManager.setPenroseConfig(penroseConfig);
        connectionManager.setPenroseContext(this);

        sourceManager = new SourceManager();
        sourceManager.setPenroseConfig(penroseConfig);
        sourceManager.setPenroseContext(this);

        moduleManager = new ModuleManager();
        moduleManager.setPenroseConfig(penroseConfig);
        moduleManager.setPenroseContext(this);
    }

    public void load() throws Exception {

        for (Iterator i=penroseConfig.getSystemPropertyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = penroseConfig.getSystemProperty(name);

            System.setProperty(name, value);
        }

        for (Iterator i=penroseConfig.getSchemaConfigs().iterator(); i.hasNext(); ) {
            SchemaConfig schemaConfig = (SchemaConfig)i.next();
            schemaManager.init(schemaConfig);
        }

        for (Iterator i=penroseConfig.getInterpreterConfigs().iterator(); i.hasNext(); ) {
            InterpreterConfig interpreterConfig = (InterpreterConfig)i.next();
            interpreterManager.init(interpreterConfig);
        }

        connectorManager.init(penroseConfig.getConnectorConfig());

        for (Iterator i=penroseConfig.getEngineConfigs().iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();
            engineManager.init(engineConfig);
        }

        for (Iterator i=penroseConfig.getHandlerConfigs().iterator(); i.hasNext(); ) {
            HandlerConfig handlerConfig = (HandlerConfig)i.next();
            handlerManager.init(handlerConfig);
        }

        for (Iterator i=penroseConfig.getPartitionConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            partitionManager.load(partitionConfig);
        }
    }


    public void store() throws Exception {

        String home = penroseConfig.getHome();
        String filename = (home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml";
        log.debug("Storing Penrose configuration into "+filename);

        PenroseConfigWriter serverConfigWriter = new PenroseConfigWriter(filename);
        serverConfigWriter.write(penroseConfig);

        partitionManager.store(home, penroseConfig.getPartitionConfigs());
    }

    public void start() throws Exception {
        connectionManager.start();
        connectorManager.start();
        engineManager.start();
        sessionManager.start();
        handlerManager.start();
        moduleManager.start();
        threadManager.start();
    }

    public void stop() throws Exception {
        threadManager.stop();
        moduleManager.stop();
        handlerManager.stop();
        sessionManager.stop();
        engineManager.stop();
        connectorManager.stop();
        connectionManager.stop();
    }

    public void clear() throws Exception {
        handlerManager.clear();
        engineManager.clear();
        connectorManager.clear();
        connectionManager.clear();
        partitionManager.clear();
        interpreterManager.clear();
        schemaManager.clear();
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }
}
