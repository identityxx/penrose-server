package org.safehaus.penrose.naming;

import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connector.ConnectorManager;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.session.SessionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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

    private PartitionValidator partitionValidator;

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

        partitionValidator = new PartitionValidator();
        partitionValidator.setPenroseConfig(penroseConfig);
        partitionValidator.setPenroseContext(this);

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

        partitionManager = new PartitionManager();
        partitionManager.setPenroseConfig(penroseConfig);
        partitionManager.setPenroseContext(this);

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
    }

    public void load() throws Exception {

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

        PartitionReader partitionReader = new PartitionReader(home);

        for (PartitionConfig partitionConfig : penroseConfig.getPartitionConfigs()) {

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("Loading "+partitionConfig.getName()+" partition.");
            }

            Partition partition = partitionReader.read(partitionConfig);

            Collection<PartitionValidationResult> results = partitionValidator.validate(partition);

            for (PartitionValidationResult result : results) {
                if (result.getType().equals(PartitionValidationResult.ERROR)) {
                    errorLog.error("ERROR: " + result.getMessage() + " [" + result.getSource() + "]");
                } else {
                    errorLog.warn("WARNING: " + result.getMessage() + " [" + result.getSource() + "]");
                }
            }

            for (ConnectionConfig connectionConfig : partition.getConnectionConfigs()) {
                Connection connection = connectionManager.init(partition, connectionConfig);
                if (connection != null) connection.start();
            }

            for (SourceConfig sourceConfig : partition.getSources().getSourceConfigs()) {
                sourceManager.init(partition, sourceConfig);
            }

            for (SourceSyncConfig sourceSyncConfig : partition.getSources().getSourceSyncConfigs()) {
                SourceSync sourceSync = sourceSyncManager.init(partition, sourceSyncConfig);
                if (sourceSync != null) sourceSync.start();
            }

            for (EntryMapping entryMapping : partition.getMappings().getEntryMappings()) {
                sourceManager.init(partition, entryMapping);
            }

            for (ModuleConfig moduleConfig : partition.getModules().getModuleConfigs()) {
                Module module = moduleManager.init(partition, moduleConfig);
                if (module != null) module.start();
            }

            partitionManager.addPartition(partition);
        }

        log.debug("----------------------------------------------------------------------------------");
    }

    public void stop() throws Exception {

        for (Partition partition : partitionManager.getPartitions()) {

            for (ModuleConfig moduleConfig : partition.getModules().getModuleConfigs()) {
                Module module = moduleManager.getModule(partition, moduleConfig.getName());
                if (module != null) module.stop();
            }

            for (SourceSyncConfig sourceSyncConfig : partition.getSources().getSourceSyncConfigs()) {
                SourceSync sourceSync = sourceSyncManager.getSourceSync(partition, sourceSyncConfig.getName());
                if (sourceSync != null) sourceSync.stop();
            }

            for (ConnectionConfig connectionConfig : partition.getConnectionConfigs()) {
                Connection connection = connectionManager.getConnection(partition, connectionConfig.getName());
                if (connection != null) connection.stop();
            }
        }

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
