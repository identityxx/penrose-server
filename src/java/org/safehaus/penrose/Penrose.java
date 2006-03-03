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

import org.apache.log4j.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.handler.SessionHandler;
import org.safehaus.penrose.session.SessionConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.module.ModuleManager;

/**
 * @author Endi S. Dewata
 */
public class Penrose {
	
    Logger log = Logger.getLogger(Penrose.class);

    public final static String PRODUCT_NAME      = "Penrose Virtual Directory Server";
    public final static String PRODUCT_VERSION   = "1.0";
    public final static String PRODUCT_COPYRIGHT = "Copyright (c) 2000-2005, Identyx Corporation.";
    public final static String VENDOR_NAME       = "Identyx Corporation";

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    private PenroseConfig penroseConfig;

    private SchemaManager schemaManager;
    private PartitionManager partitionManager;
    private PartitionValidator partitionValidator;
    private ConnectionManager connectionManager;
    private ModuleManager moduleManager;
    private SessionManager sessionManager;

    private InterpreterFactory interpreterFactory;
    private Connector connector;
	private Engine engine;

    private SessionHandler sessionHandler;

    private String status = STOPPED;

    public Penrose(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;
        init();
        load();
    }

    public Penrose(String home) throws Exception {
        penroseConfig = new PenroseConfig();
        init();
        load(home);
    }

    public Penrose() throws Exception {
        this((String)null);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialize Penrose components
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void init() throws Exception {

        initSchemaManager();
        initSessionManager();

        initPartitionManager();
        initConnectionManager();
        initModuleManager();
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
        loadModules();

        loadConnector();
        loadEngine();
        loadSessionHandler();
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
        interpreterFactory = new InterpreterFactory(interpreterConfig);
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

        if (connector != null) return;

        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();

        Class clazz = Class.forName(connectorConfig.getConnectorClass());
        connector = (Connector)clazz.newInstance();

        connector.setConnectorConfig(connectorConfig);
        connector.setPenroseConfig(penroseConfig);
        connector.setConnectionManager(connectionManager);
        connector.setPartitionManager(partitionManager);
        connector.init();
    }

    public void loadEngine() throws Exception {

        if (engine != null) return;

        EngineConfig engineConfig = penroseConfig.getEngineConfig();

        Class clazz = Class.forName(engineConfig.getEngineClass());
        engine = (Engine)clazz.newInstance();

        engine.setEngineConfig(engineConfig);
        engine.setPenroseConfig(penroseConfig);
        engine.setSchemaManager(schemaManager);
        engine.setInterpreterFactory(interpreterFactory);
        engine.setConnector(connector);
        engine.setConnectionManager(connectionManager);
        engine.setPartitionManager(partitionManager);
        engine.init();
    }

    public void loadSessionHandler() throws Exception {

        if (sessionHandler != null) return;

        sessionHandler = new SessionHandler();
        sessionHandler.setSessionManager(sessionManager);
        sessionHandler.setSchemaManager(schemaManager);
        sessionHandler.setInterpreterFactory(interpreterFactory);
        sessionHandler.setEngine(engine);
        sessionHandler.setRootUserConfig(penroseConfig.getRootUserConfig());
        sessionHandler.setPartitionManager(partitionManager);
        sessionHandler.setModuleManager(moduleManager);
    }


    public void clear() throws Exception {
        sessionHandler = null;
        engine = null;
        connector = null;

        connectionManager.clear();
        partitionManager.clear();
        schemaManager.clear();
        penroseConfig.clear();
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
            connector.start();
            engine.start();
            sessionHandler.start();
            moduleManager.start();

            status = STARTED;

        } catch (Exception e) {
            status = STOPPED;
            log.debug(e.getMessage(), e);
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

            moduleManager.stop();
            sessionHandler.stop();
            engine.stop();
            connector.stop();
            connectionManager.stop();

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
        }

        status = STOPPED;
	}

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Penrose Sessions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseSession newSession() throws Exception {
        return sessionHandler.newSession();
    }

    public void closeSession(PenroseSession session) {
        sessionHandler.closeSession(session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Setters & Getters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public InterpreterFactory getInterpreterFactory() {
        return interpreterFactory;
    }

    public void setInterpreterFactory(InterpreterFactory interpreterFactory) {
        this.interpreterFactory = interpreterFactory;
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
        return engine;
    }

    public Connector getConnector() {
        return connector;
    }

    public SessionHandler getSessionHandler() {
        return sessionHandler;
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
}
