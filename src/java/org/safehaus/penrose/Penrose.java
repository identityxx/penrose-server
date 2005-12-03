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

import org.apache.log4j.Logger;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.session.PenroseSessionManager;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.session.PenroseSessionManager;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.ietf.ldap.LDAPSearchConstraints;
import org.ietf.ldap.LDAPConnection;

/**
 * @author Endi S. Dewata
 */
public class Penrose {
	
    Logger log = Logger.getLogger(Penrose.class);

    public final static String PRODUCT_NAME      = "Penrose Virtual Directory Server";
    public final static String PRODUCT_VERSION   = "0.9.8";
    public final static String PRODUCT_COPYRIGHT = "Copyright (c) 2000-2005, Identyx Corporation.";
    public final static String VENDOR_NAME       = "Identyx Corporation";

    private PenroseConfig penroseConfig;

    private SchemaManager schemaManager;
    private PartitionManager partitionManager;
    private PartitionValidator partitionValidator;
    private ConnectionManager connectionManager;

    private InterpreterFactory interpreterFactory;
    private Connector connector;
	private Engine engine;
    private Handler handler;

	private PenroseSessionManager sessionManager;

    private boolean stopRequested = false;

	public Penrose() throws Exception {
        this((String)null);
    }

    public Penrose(String home) throws Exception {

        PenroseConfigReader reader = new PenroseConfigReader((home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml");
        penroseConfig = reader.read();
        penroseConfig.setHome(home);

        init();
    }

    public Penrose(PenroseConfig penroseConfig) throws Exception {
        this.penroseConfig = penroseConfig;

        init();
    }

    void init() throws Exception {

        initSystemProperties();
        initSchemaManager();
        initPartitionManager();
	}

    public void initSystemProperties() throws Exception {
        for (Iterator i=penroseConfig.getSystemPropertyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = penroseConfig.getSystemProperty(name);

            System.setProperty(name, value);
        }
    }

    public void initSchemaManager() throws Exception {

        schemaManager = new SchemaManager();
        schemaManager.setHome(penroseConfig.getHome());

        for (Iterator i=penroseConfig.getSchemaConfigs().iterator(); i.hasNext(); ) {
            SchemaConfig schemaConfig = (SchemaConfig)i.next();

            schemaManager.load(schemaConfig);
        }
    }

    public void initPartitionManager() throws Exception {
        partitionManager = new PartitionManager();
        partitionManager.setHome(penroseConfig.getHome());
        partitionManager.setSchemaManager(schemaManager);

        partitionValidator = new PartitionValidator();
        partitionValidator.setPenroseConfig(penroseConfig);
        partitionValidator.setSchemaManager(schemaManager);

    }

	public void start() throws Exception {

        stopRequested = false;

        loadPartitions();

        initInterpreter();
        initConnections();
        initConnectors();
        initEngines();
        initHandler();
        initSessionManager();
	}

    public void loadPartitions() throws Exception {

        for (Iterator i=penroseConfig.getPartitionConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            Partition partition = partitionManager.load(partitionConfig);

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

    public void storePartitions() throws Exception {

        for (Iterator i=penroseConfig.getPartitionConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            partitionManager.store(partitionConfig);
        }
    }

    public void initInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = penroseConfig.getInterpreterConfig();
        interpreterFactory = new InterpreterFactory(interpreterConfig);
    }

    public void initConnections() throws Exception {
        connectionManager = new ConnectionManager();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            Collection connectionConfigs = partition.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();
                connectionManager.addConnectionConfig(connectionConfig);
            }
        }

        connectionManager.init();
    }

    public void initConnectors() throws Exception {

        ConnectorConfig connectorConfig = penroseConfig.getConnectorConfig();

        Class clazz = Class.forName(connectorConfig.getConnectorClass());
        connector = (Connector)clazz.newInstance();

        connector.setServerConfig(penroseConfig);
        connector.setConnectionManager(connectionManager);
        connector.init(connectorConfig);
        connector.setPartitionManager(partitionManager);

        connector.start();
    }

    public void initEngines() throws Exception {

        EngineConfig engineConfig = penroseConfig.getEngineConfig();

        Class clazz = Class.forName(engineConfig.getEngineClass());
        engine = (Engine)clazz.newInstance();

        engine.setServerConfig(penroseConfig);
        engine.setSchemaManager(schemaManager);
        engine.setInterpreterFactory(interpreterFactory);
        engine.setConnector(connector);
        engine.setConnectionManager(connectionManager);
        engine.init(engineConfig);
        engine.setPartitionManager(partitionManager);

        engine.start();
    }

    public void initHandler() throws Exception {
        handler = new Handler();
        handler.setSchemaManager(schemaManager);
        handler.setInterpreterFactory(interpreterFactory);
        handler.setEngine(engine);
        handler.setRootUserConfig(penroseConfig.getRootUserConfig());
        handler.setPartitionManager(partitionManager);

        handler.init();
    }

    public void initSessionManager() throws Exception {
        sessionManager = new PenroseSessionManager(this);
    }

	public void stop() {
        
        if (stopRequested) return;

        try {
            //log.debug("Stop requested...");
            stopRequested = true;

            engine.stop();
            connector.stop();

            //connectionManager.stop();

            //log.warn("Penrose has been shutdown.");

        } catch (Exception e) {
            e.printStackTrace();
        }
	}

    public PenroseSession newSession() throws Exception {
        return sessionManager.createConnection();
    }

    public void removeSession(PenroseSession session) {
        sessionManager.removeConnection(session);
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    public void setStopRequested(boolean stopRequested) {
        this.stopRequested = stopRequested;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public InterpreterFactory getInterpreterFactory() {
        return interpreterFactory;
    }

    public void setInterpreterFactory(InterpreterFactory interpreterFactory) {
        this.interpreterFactory = interpreterFactory;
    }

    public PartitionManager getConfigManager() {
        return partitionManager;
    }

    public void setConfigManager(PartitionManager partitionManager) {
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

    public Handler getHandler() {
        return handler;
    }

    public void create() throws Exception {
        connector.create();

        //createMappingsTable();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            create(partition, null, entryDefinitions);
        }
    }

    public void create(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Creating tables for "+entryMapping.getDn());
            EntryCache cache = engine.getCache(parentDn, entryMapping);
            cache.create();

            Collection children = partition.getChildren(entryMapping);
            create(partition, entryMapping.getDn(), children);
        }
    }

    public void load() throws Exception {
        connector.load();

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            load(partition);
        }
    }

    public void load(Partition partition) throws Exception {
        Collection entryDefinitions = partition.getRootEntryMappings();
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Loading entries under "+entryMapping.getDn());

            PenroseSearchResults sr = handler.search(
                    null,
                    entryMapping.getDn(),
                    LDAPConnection.SCOPE_SUB,
                    LDAPSearchConstraints.DEREF_NEVER,
                    "(objectClass=*)",
                    new ArrayList()
            );

            while (sr.hasNext()) sr.next();
        }
    }

    public void clean() throws Exception {

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            clean(partition, null, entryDefinitions);
        }

        connector.clean();
    }

    public void clean(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            clean(partition, entryMapping.getDn(), children);

            log.debug("Cleaning tables for "+entryMapping.getDn());
            EntryCache cache = engine.getCache(parentDn, entryMapping);
            cache.clean();
        }
    }

    public void drop() throws Exception {

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryDefinitions = partition.getRootEntryMappings();
            drop(partition, null, entryDefinitions);
        }

        //dropMappingsTable();

        connector.drop();
    }

    public void drop(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            drop(partition, entryMapping.getDn(), children);

            log.debug("Deleting entries under "+entryMapping.getDn());
            EntryCache cache = engine.getCache(parentDn, entryMapping);
            cache.drop();
        }
    }

    public static void main(String args[]) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: org.safehaus.penrose.Penrose [command]");
            System.out.println();
            System.out.println("Commands:");
            System.out.println("    create - create cache tables");
            System.out.println("    load   - load data into cache tables");
            System.out.println("    clean  - clean data from cache tables");
            System.out.println("    drop   - drop cache tables");
            System.exit(0);
        }

        String homeDirectory = System.getProperty("penrose.home");

        String command = args[0];

        Penrose penrose = new Penrose(homeDirectory);
        penrose.start();

        if ("create".equals(command)) {
            penrose.create();

        } else if ("load".equals(command)) {
            penrose.load();

        } else if ("clean".equals(command)) {
            penrose.clean();

        } else if ("drop".equals(command)) {
            penrose.drop();

        } else if ("run".equals(command)) {
            //penrose.start();
        }

        penrose.stop();
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
}
