/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerManager;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.session.SessionConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.event.EventManager;

/**
 * @author Endi S. Dewata
 */
public class Penrose {

    Logger log = Logger.getLogger(Penrose.class);

    public final static String PRODUCT_NAME      = "Penrose Virtual Directory Server";
    public final static String PRODUCT_VERSION   = "1.0";
    public final static String PRODUCT_COPYRIGHT = "Copyright (c) 2000-2006, Identyx Corporation.";
    public final static String VENDOR_NAME       = "Identyx Corporation";

    public final static DateFormat DATE_FORMAT   = new SimpleDateFormat("MM/dd/yyyy");
    public final static String RELEASE_DATE      = "04/01/2006";

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private PenroseConfig penroseConfig;

    private ThreadManager threadManager;
    private SchemaManager schemaManager;
    private PartitionManager partitionManager;
    private PartitionValidator partitionValidator;
    private ConnectionManager connectionManager;
    private ModuleManager moduleManager;
    private SessionManager sessionManager;

    private ConnectorManager connectorManager;
    private EngineManager engineManager;
    private EventManager eventManager;
    private HandlerManager handlerManager;

    private InterpreterManager interpreterManager;

    private String status = STOPPED;

    protected Penrose(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;
        init();
        load();
    }

    protected Penrose(String home) throws Exception {

        PenroseConfigReader reader = new PenroseConfigReader((home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml");
        penroseConfig = reader.read();
        penroseConfig.setHome(home);

        init();
        load(home);
    }

    protected Penrose() throws Exception {
        penroseConfig = new PenroseConfig();
        init();
        load(null);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialize Penrose components
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void init() throws Exception {
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

    public void initThreadManager() throws Exception {
        //String s = engineConfig.getParameter(EngineConfig.THREAD_POOL_SIZE);
        //int threadPoolSize = s == null ? EngineConfig.DEFAULT_THREAD_POOL_SIZE : Integer.parseInt(s);

        threadManager = new ThreadManager(50);
    }

    public void initSchemaManager() throws Exception {
        schemaManager = new SchemaManager();
    }

    public void initSessionManager() throws Exception {
        SessionConfig sessionConfig = penroseConfig.getSessionConfig();
        sessionManager = new SessionManager(sessionConfig);
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
        connectorManager.setThreadManager(threadManager);
    }

    public void initEngineManager() throws Exception {
        engineManager = new EngineManager();
        engineManager.setPenroseConfig(penroseConfig);
        engineManager.setSchemaManager(schemaManager);
        engineManager.setInterpreterFactory(interpreterManager);
        engineManager.setConnectionManager(connectionManager);
        engineManager.setPartitionManager(partitionManager);
        engineManager.setThreadManager(threadManager);
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

    public void load(String home) throws Exception {

        String filename = (home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml";
        log.debug("Loading Penrose configuration from "+filename);

        PenroseConfigReader reader = new PenroseConfigReader(filename);
        reader.read(penroseConfig);
        penroseConfig.setHome(home);

        load();
    }

    public void load() throws Exception {

        loadSystemProperties();

        loadInterpreter();
        loadSchemas();

        loadPartitions();
        loadConnections();

        loadConnector();
        loadEngine();
        loadHandler();

        loadModules();
    }

    public void loadSystemProperties() throws Exception {
        for (Iterator i=penroseConfig.getSystemPropertyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = penroseConfig.getSystemProperty(name);

            System.setProperty(name, value);
        }
    }

    public void loadInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = penroseConfig.getInterpreterConfig();
        interpreterManager.init(interpreterConfig);
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

    public void loadEngine() throws Exception {

        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();
        Connector connector = connectorManager.getConnector(connectorConfig.getName());

        Collection engineConfigs = penroseConfig.getEngineConfigs();

        for (Iterator i=engineConfigs.iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();
            if (engineManager.getEngine(engineConfig.getName()) != null) continue;

            engineManager.init(engineConfig, connector);
        }
    }

    public void loadHandler() throws Exception {

        HandlerConfig handlerConfig = penroseConfig.getHandlerConfig();
        if (handlerManager.getHandler(handlerConfig.getName()) != null) return;

        handlerManager.init(handlerConfig, engineManager);
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
        load(penroseConfig.getHome());
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

            threadManager.stopRequestAllWorkers();

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

        HandlerConfig handlerConfig = penroseConfig.getHandlerConfig();
        Handler handler = handlerManager.getHandler(handlerConfig.getName());
        session.setHandler(handler);

        session.setEventManager(eventManager);

        return session;
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
        EngineConfig engineConfig = penroseConfig.getEngineConfig();
        return engineManager.getEngine(engineConfig.getName());
    }

    public Connector getConnector() {
        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();
        return connectorManager.getConnector(connectorConfig.getName());
    }

    public Handler getSessionHandler() {
        HandlerConfig handlerConfig = penroseConfig.getHandlerConfig();
        return handlerManager.getHandler(handlerConfig.getName());
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

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }

    public EventManager getEventManager() {
        return eventManager;
    }

    public void setEventManager(EventManager eventManager) {
        this.eventManager = eventManager;
    }
}
