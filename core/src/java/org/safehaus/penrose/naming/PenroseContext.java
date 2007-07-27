package org.safehaus.penrose.naming;

import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connector.ConnectorManager;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.session.SessionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PenroseContext {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

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

    private String             home;
    private PenroseConfig      penroseConfig;

    private ThreadManager      threadManager;
    private SchemaManager      schemaManager;
    private FilterEvaluator    filterEvaluator;
    private InterpreterManager interpreterManager;

    private ConnectorManager   connectorManager;

    private PartitionManager   partitionManager;
    private ConnectionManager  connectionManager;
    private SourceManager      sourceManager;
    private SourceSyncManager  sourceSyncManager;

    private SessionContext     sessionContext;
    private ModuleManager      moduleManager;

    public PenroseContext(String home) {
        this.home = home;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
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

    public SourceManager getSourceManager() {
        return sourceManager;
    }

    public void setSourceManager(SourceManager sourceManager) {
        this.sourceManager = sourceManager;
    }

    public SourceSyncManager getSourceSyncManager() {
        return sourceSyncManager;
    }

    public void setSourceSyncManager(SourceSyncManager sourceSyncManager) {
        this.sourceSyncManager = sourceSyncManager;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

    public void init(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;

        threadManager = new ThreadManager();
        threadManager.setPenroseConfig(penroseConfig);
        threadManager.setPenroseContext(this);

        schemaManager = new SchemaManager();
        schemaManager.setPenroseConfig(penroseConfig);
        schemaManager.setPenroseContext(this);

        filterEvaluator = new FilterEvaluator();
        filterEvaluator.setSchemaManager(schemaManager);

        interpreterManager = new InterpreterManager();
        interpreterManager.setPenroseConfig(penroseConfig);
        interpreterManager.setPenroseContext(this);

        connectorManager = new ConnectorManager();
        connectorManager.setPenroseConfig(penroseConfig);
        connectorManager.setPenroseContext(this);

        connectionManager = new ConnectionManager();
        connectionManager.setPenroseConfig(penroseConfig);
        connectionManager.setPenroseContext(this);

        sourceManager = new SourceManager();
        sourceManager.setPenroseConfig(penroseConfig);
        sourceManager.setPenroseContext(this);

        sourceSyncManager = new SourceSyncManager();
        sourceSyncManager.setPenroseConfig(penroseConfig);
        sourceSyncManager.setPenroseContext(this);

        moduleManager = new ModuleManager();
        moduleManager.setPenroseConfig(penroseConfig);
        moduleManager.setPenroseContext(this);
        moduleManager.setSessionContext(sessionContext);

        partitionManager = new PartitionManager();
        partitionManager.setPenroseConfig(penroseConfig);
        partitionManager.setPenroseContext(this);

        partitionManager.setConnectionManager(connectionManager);
        partitionManager.setSourceManager(sourceManager);
        partitionManager.setSourceSyncManager(sourceSyncManager);
        partitionManager.setModuleManager(moduleManager);

        for (String name : penroseConfig.getSystemPropertyNames()) {
            String value = penroseConfig.getSystemProperty(name);

            System.setProperty(name, value);
        }

        for (SchemaConfig schemaConfig : penroseConfig.getSchemaConfigs()) {
            schemaManager.init(home, schemaConfig);
        }

        for (InterpreterConfig interpreterConfig : penroseConfig.getInterpreterConfigs()) {
            interpreterManager.init(interpreterConfig);
        }

        connectorManager.init(penroseConfig.getConnectorConfig());
    }

    public void start() throws Exception {
        threadManager.start();

        String dir = (home == null ? "" : home+ File.separator)+"partitions";

        File partitions = new File(dir);
        if (!partitions.isDirectory()) return;

        for (File file : partitions.listFiles()) {
            if (!file.isDirectory()) continue;

            if (debug) log.debug("----------------------------------------------------------------------------------");

            PartitionConfig partitionConfig = partitionManager.load(file);
            String name = partitionConfig.getName();

            if (!partitionConfig.isEnabled()) {
                log.debug(name+" partition is disabled.");
                continue;
            }

            log.debug("Starting "+name+" partition.");

            partitionManager.init(partitionConfig);
        }
    }

    public void stop() throws Exception {
        partitionManager.clear();
        threadManager.stop();
    }

    public void clear() throws Exception {
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

    public FilterEvaluator getFilterEvaluator() {
        return filterEvaluator;
    }

    public void setFilterEvaluator(FilterEvaluator filterEvaluator) {
        this.filterEvaluator = filterEvaluator;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }
}
