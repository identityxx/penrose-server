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
package org.safehaus.penrose;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.safehaus.penrose.config.*;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.event.EventManager;
import org.safehaus.penrose.log4j.Log4jConfigReader;
import org.safehaus.penrose.log4j.Log4jConfig;
import org.safehaus.penrose.log4j.LoggerConfig;
import org.safehaus.penrose.log4j.AppenderConfig;
import org.safehaus.penrose.naming.PenroseInitialContextFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;

/**
 * @author Endi S. Dewata
 */
public class Penrose {

    Logger log = LoggerFactory.getLogger(getClass());

    public static String PRODUCT_NAME          = "Penrose";
    public static String PRODUCT_VERSION       = "1.2";
    public static String VENDOR_NAME           = "Identyx";
    public static String PRODUCT_COPYRIGHT     = "Copyright (c) 2000-2007, Identyx Corporation.";
    public static String SPECIFICATION_VERSION = "1.2";

    public final static DateFormat DATE_FORMAT   = new SimpleDateFormat("MM/dd/yyyy");
    public final static String RELEASE_DATE      = "12/15/2006";

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private PenroseConfig      penroseConfig;

    private ThreadGroup        threadGroup;
    private ThreadPoolExecutor executorService;

    private SchemaManager      schemaManager;
    private PartitionManager   partitionManager;
    private PartitionValidator partitionValidator;
    private ConnectionManager  connectionManager;
    private ModuleManager      moduleManager;
    private SessionManager     sessionManager;

    private ConnectorManager   connectorManager;
    private EngineManager      engineManager;
    private EventManager       eventManager;
    private HandlerManager     handlerManager;

    private InterpreterManager interpreterManager;

    private String status = STOPPED;

    Context context;

    static {
        try {
            Package pkg = Penrose.class.getPackage();

            String value = pkg.getImplementationTitle();
            if (value != null) PRODUCT_NAME = value;

            value = pkg.getImplementationVersion();
            if (value != null) PRODUCT_VERSION = value;

            value = pkg.getImplementationVendor();
            if (value != null) VENDOR_NAME = value;

            value = pkg.getSpecificationVersion();
            if (value != null) SPECIFICATION_VERSION = value;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Penrose(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;
        init();
        load();
    }

    protected Penrose(String home) throws Exception {

        penroseConfig = new PenroseConfig();
        penroseConfig.setHome(home);
        loadConfig();

        init();
        load();
    }

    protected Penrose() throws Exception {
        penroseConfig = new PenroseConfig();
        loadConfig();

        init();
        load();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialize Penrose components
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void init() throws Exception {

        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, PenroseInitialContextFactory.class.getName());
        context = new InitialContext(env);
        
        initLoggers();
        initThreadManager();
        initSchemaManager();
        initSessionManager();

        initPartitionManager();
        initConnectionManager();
        initModuleManager();

        initInterpreterManager();
        initConnectorManager();
        initEngineManager();
        initEventManager();
        initHandlerManager();
    }

    public void initLoggers() throws Exception {

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
    }

    public void initThreadManager() throws Exception {
        String s = penroseConfig.getProperty("maxThreads");
        int maxThreads = s == null ? 20 : Integer.parseInt(s);

        log.debug("Initializing ThreadManager("+ maxThreads +")");

        threadGroup = new ThreadGroup("Penrose");

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue()
        );

        threadPoolExecutor.setThreadFactory(new ThreadFactory() {
            AtomicInteger threadId = new AtomicInteger();
            public Thread newThread(Runnable r) {
                return new Thread(threadGroup, r, threadGroup.getName()+"-"+threadId.getAndIncrement());
            }
        });
        
        executorService = threadPoolExecutor;
        context.bind("java:comp/ExecutorService", executorService);
    }

    public void initSchemaManager() throws Exception {
        schemaManager = new SchemaManager();
    }

    public void initSessionManager() throws Exception {
        sessionManager = new SessionManager();
        sessionManager.setPenrose(this);
        sessionManager.setPenroseConfig(penroseConfig);
    }

    public void initPartitionManager() throws Exception {
        partitionManager = new PartitionManager();
        partitionManager.setSchemaManager(schemaManager);

        partitionValidator = new PartitionValidator();
        partitionValidator.setPenroseConfig(penroseConfig);
        partitionValidator.setSchemaManager(schemaManager);
    }

    public void initConnectionManager() throws Exception {
        connectionManager = new ConnectionManager();
    }

    public void initModuleManager() throws Exception {
        moduleManager = new ModuleManager();
        moduleManager.setPenrose(this);
    }

    public void initInterpreterManager() throws Exception {
        interpreterManager = new InterpreterManager();
    }

    public void initConnectorManager() throws Exception {
        connectorManager = new ConnectorManager();
        connectorManager.setPenroseConfig(penroseConfig);
        connectorManager.setConnectionManager(connectionManager);
        connectorManager.setPartitionManager(partitionManager);
    }

    public void initEngineManager() throws Exception {
        engineManager = new EngineManager();
        engineManager.setPenrose(this);
        engineManager.setPenroseConfig(penroseConfig);
        engineManager.setSchemaManager(schemaManager);
        engineManager.setInterpreterFactory(interpreterManager);
        engineManager.setConnectorManager(connectorManager);
        engineManager.setConnectionManager(connectionManager);
        engineManager.setPartitionManager(partitionManager);
    }

    public void initEventManager() throws Exception {
        eventManager = new EventManager();
        eventManager.setModuleManager(moduleManager);
    }

    public void initHandlerManager() throws Exception {
        handlerManager = new HandlerManager();
        handlerManager.setPenroseConfig(penroseConfig);
        handlerManager.setSessionManager(sessionManager);
        handlerManager.setSchemaManager(schemaManager);
        handlerManager.setInterpreterFactory(interpreterManager);
        handlerManager.setPartitionManager(partitionManager);
        handlerManager.setModuleManager(moduleManager);
        handlerManager.setPenrose(this);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Load Penrose Configurations
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void loadConfig() throws Exception {

        String home = penroseConfig.getHome();

        penroseConfig.clear();

        PenroseConfigReader reader = new PenroseConfigReader((home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml");
        reader.read(penroseConfig);
        penroseConfig.setHome(home);
    }

    public void load() throws Exception {

        loadSystemProperties();

        loadInterpreters();
        loadSchemas();

        loadPartitions();
        loadConnections();

        loadConnector();
        loadEngines();
        loadHandlers();

        loadModules();
    }

    public void loadSystemProperties() throws Exception {
        for (Iterator i=penroseConfig.getSystemPropertyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = penroseConfig.getSystemProperty(name);

            System.setProperty(name, value);
        }
    }

    public void loadInterpreters() throws Exception {
        for (Iterator i=penroseConfig.getInterpreterConfigs().iterator(); i.hasNext(); ) {
            InterpreterConfig interpreterConfig = (InterpreterConfig)i.next();
            interpreterManager.init(interpreterConfig);
        }
    }

    public void loadSchemas() throws Exception {
        schemaManager.load(penroseConfig.getHome(), penroseConfig.getSchemaConfigs());
    }

    public void loadPartitions() throws Exception {
        Collection newPartitions = partitionManager.load(penroseConfig.getHome(), penroseConfig.getPartitionConfigs());

        for (Iterator i=newPartitions.iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection results = partitionValidator.validate(partition);

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                PartitionValidationResult resultPartition = (PartitionValidationResult)j.next();

                if (resultPartition.getType().equals(PartitionValidationResult.ERROR)) {
                    log.error("ERROR: "+resultPartition.getMessage()+" ["+resultPartition.getSource()+"]");
                } else {
                    log.warn("WARNING: "+resultPartition.getMessage()+" ["+resultPartition.getSource()+"]");
                }
            }
        }
    }

    public void loadConnections() throws Exception {
        Collection partitions = partitionManager.getPartitions();
        for (Iterator i=partitions.iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            Collection connectionConfigs = partition.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();

                String adapterName = connectionConfig.getAdapterName();
                if (adapterName == null) throw new Exception("Missing adapter name");

                AdapterConfig adapterConfig = penroseConfig.getAdapterConfig(adapterName);
                if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

                connectionManager.init(partition, connectionConfig, adapterConfig);
            }
        }
    }

    public void loadModules() throws Exception {
        moduleManager.load(partitionManager.getPartitions());
    }

    public void loadConnector() throws Exception {

        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();
        Connector connector = connectorManager.getConnector(connectorConfig.getName());

        if (connector != null) return;

        connectorManager.init(connectorConfig);
    }

    public void loadEngines() throws Exception {

        Collection engineConfigs = penroseConfig.getEngineConfigs();
        for (Iterator i=engineConfigs.iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();
            if (engineManager.getEngine(engineConfig.getName()) != null) continue;

            engineManager.init(engineConfig);
        }
    }

    public void loadHandlers() throws Exception {

        Collection handlerConfigs = penroseConfig.getHandlerConfigs();
        for (Iterator i=handlerConfigs.iterator(); i.hasNext(); ) {
            HandlerConfig handlerConfig = (HandlerConfig)i.next();
            if (handlerManager.getHandler(handlerConfig.getName()) != null) return;

            handlerManager.init(handlerConfig, engineManager);
        }
    }


    public void clear() throws Exception {
        handlerManager.clear();
        engineManager.clear();
        connectorManager.clear();
        interpreterManager.clear();
        connectionManager.clear();
        partitionManager.clear();
        schemaManager.clear();
    }

    public void reload() throws Exception {
        clear();
        loadConfig();
        init();
        load();
    }

    public void store() throws Exception {

        String home = penroseConfig.getHome();
        String filename = (home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml";
        log.debug("Storing Penrose configuration into "+filename);

        PenroseConfigWriter serverConfigWriter = new PenroseConfigWriter(filename);
        serverConfigWriter.write(penroseConfig);

        partitionManager.store(home, penroseConfig.getPartitionConfigs());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Start Penrose
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void start() throws Exception {

        if (status != STOPPED) return;

        try {
            status = STARTING;

            loadPartitions();
            loadConnections();
            loadModules();

            connectionManager.start();
            connectorManager.start();
            engineManager.start();
            sessionManager.start();
            handlerManager.start();
            moduleManager.start();

            status = STARTED;

        } catch (Exception e) {
            status = STOPPED;
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Stop Penrose
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void stop() {

        if (status != STARTED) return;

        try {
            status = STOPPING;

            executorService.shutdown();
            
            moduleManager.stop();
            handlerManager.stop();
            sessionManager.stop();
            engineManager.stop();
            connectorManager.stop();
            connectionManager.stop();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        status = STOPPED;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Penrose Sessions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseSession newSession() throws Exception {

        PenroseSession session = sessionManager.newSession();
        if (session == null) return null;

        session.setHandlerManager(handlerManager);
        session.setEventManager(eventManager);
        session.setSchemaManager(schemaManager);
        session.setPartitionManager(partitionManager);

        return session;
    }

    public PenroseSession createSession(Object sessionId) throws Exception {

        PenroseSession session = sessionManager.createSession(sessionId);
        if (session == null) return null;

        session.setHandlerManager(handlerManager);
        session.setEventManager(eventManager);
        session.setSchemaManager(schemaManager);
        session.setPartitionManager(partitionManager);

        return session;
    }

    public PenroseSession getSession(String sessionId) throws Exception {
        return sessionManager.getSession(sessionId);
    }

    public PenroseSession removeSession(String sessionId) throws Exception {
        return sessionManager.removeSession(sessionId);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Setters & Getters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public InterpreterManager getInterpreterFactory() {
        return interpreterManager;
    }

    public void setInterpreterFactory(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
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

    public Engine getEngine() {
        return engineManager.getEngine("DEFAULT");
    }

    public Connector getConnector() {
        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();
        return connectorManager.getConnector(connectorConfig.getName());
    }

    public Handler getHandler() {
        return handlerManager.getHandler("DEFAULT");
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public String getStatus() {
        return status;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public ExecutorService getExecutorService() {
        return executorService;
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

    public HandlerManager getHandlerManager() {
        return handlerManager;
    }

    public void setHandlerManager(HandlerManager handlerManager) {
        this.handlerManager = handlerManager;
    }
}
