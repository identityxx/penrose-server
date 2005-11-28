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
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.client.ClientManager;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.ietf.ldap.LDAPSearchConstraints;
import org.ietf.ldap.LDAPConnection;

/**
 * @author Endi S. Dewata
 */
public class Penrose {
	
    Logger log = Logger.getLogger(Penrose.class);

    public final static String PRODUCT_NAME      = "Penrose Virtual Directory Server 0.9.8";
    public final static String PRODUCT_COPYRIGHT = "Copyright (c) 2000-2005, Identyx Corporation.";
    public final static String VENDOR_NAME       = "Identyx Corporation";

    private String home;

    private PenroseConfig config;
    private Schema schema;

    private PartitionManager partitionManager;
    private ConnectionManager connectionManager;

    private InterpreterFactory interpreterFactory;
    private Connector connector;
	private Engine engine;
    private Handler handler;

	private ClientManager clientManager;

    private boolean stopRequested = false;

	public Penrose() {
	}

	public void start() throws Exception {

        stopRequested = false;

        loadServerConfig();
        loadSystemProperties();

        loadSchema();
        loadConfigs();

        initInterpreter();
        initConnections();
        initConnectors();
        initEngines();
        initHandler();
        initClientManager();
	}

    public void loadServerConfig() throws Exception {
        if (config == null) {
            PenroseConfigReader reader = new PenroseConfigReader();
            config = reader.read((home == null ? "" : home+File.separator)+"conf"+File.separator+"server.xml");
        }
    }

    public void loadSystemProperties() throws Exception {
        for (Iterator i=config.getSystemPropertyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = config.getSystemProperty(name);

            System.setProperty(name, value);
        }
    }

    public void loadSchema() throws Exception {
        SchemaReader reader = new SchemaReader();
        reader.readDirectory((home == null ? "" : home+File.separator)+"schema");
        reader.readDirectory((home == null ? "" : home+File.separator)+"schema"+File.separator+"ext");
        schema = reader.getSchema();
    }

    public void loadConfigs() throws Exception {

        partitionManager = new PartitionManager();
        partitionManager.setServerConfig(config);
        partitionManager.setSchema(schema);
        partitionManager.init();

        partitionManager.load((home == null ? "" : home+File.separator)+"conf");

        File partitions = new File((home == null ? "" : home+File.separator)+"partitions");
        if (partitions.exists()) {
            File files[] = partitions.listFiles();
            for (int i=0; i<files.length; i++) {
                File partition = files[i];

                partitionManager.load(partition.getAbsolutePath());
            }
        }
    }

    public void initInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = config.getInterpreterConfig();
        interpreterFactory = new InterpreterFactory(interpreterConfig);
    }

    public void initConnections() throws Exception {
        connectionManager = new ConnectionManager();

        for (Iterator i=partitionManager.getConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();

            Collection connectionConfigs = partitionConfig.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();
                connectionManager.addConnectionConfig(connectionConfig);
            }
        }

        connectionManager.init();
    }

    public void initConnectors() throws Exception {

        ConnectorConfig connectorConfig = config.getConnectorConfig();

        Class clazz = Class.forName(connectorConfig.getConnectorClass());
        connector = (Connector)clazz.newInstance();

        connector.setServerConfig(config);
        connector.setConnectionManager(connectionManager);
        connector.init(connectorConfig);
        connector.setConfigManager(partitionManager);

        connector.start();
    }

    public void initEngines() throws Exception {

        EngineConfig engineConfig = config.getEngineConfig();

        Class clazz = Class.forName(engineConfig.getEngineClass());
        engine = (Engine)clazz.newInstance();

        engine.setServerConfig(config);
        engine.setSchema(schema);
        engine.setInterpreterFactory(interpreterFactory);
        engine.setConnector(connector);
        engine.setConnectionManager(connectionManager);
        engine.init(engineConfig);
        engine.setConfigManager(partitionManager);

        engine.start();
    }

    public void initHandler() throws Exception {
        handler = new Handler();
        handler.setSchema(schema);
        handler.setInterpreterFactory(interpreterFactory);
        handler.setEngine(engine);
        handler.setRootDn(config.getRootDn());
        handler.setRootPassword(config.getRootPassword());
        handler.setConfigManager(partitionManager);

        handler.init();
    }

    public void initClientManager() throws Exception {
        clientManager = new ClientManager(this);
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

    public PenroseConnection openConnection() throws Exception {
        return clientManager.createConnection();
    }

    public void removeConnection(PenroseConnection connection) {
        clientManager.removeConnection(connection);
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    public void setStopRequested(boolean stopRequested) {
        this.stopRequested = stopRequested;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public PenroseConfig getConfig() {
        return config;
    }

    public void setConfig(PenroseConfig config) {
        this.config = config;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
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

        for (Iterator i=partitionManager.getConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            Collection entryDefinitions = partitionConfig.getRootEntryMappings();
            create(partitionConfig, null, entryDefinitions);
        }
    }

    public void create(PartitionConfig partitionConfig, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Creating tables for "+entryMapping.getDn());
            EntryCache cache = engine.getCache(parentDn, entryMapping);
            cache.create();

            Collection children = partitionConfig.getChildren(entryMapping);
            create(partitionConfig, entryMapping.getDn(), children);
        }
    }

    public void load() throws Exception {
        connector.load();

        for (Iterator i=partitionManager.getConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            load(partitionConfig);
        }
    }

    public void load(PartitionConfig partitionConfig) throws Exception {
        Collection entryDefinitions = partitionConfig.getRootEntryMappings();
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Loading entries under "+entryMapping.getDn());

            SearchResults sr = handler.search(
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

        for (Iterator i=partitionManager.getConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            Collection entryDefinitions = partitionConfig.getRootEntryMappings();
            clean(partitionConfig, null, entryDefinitions);
        }

        connector.clean();
    }

    public void clean(PartitionConfig partitionConfig, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partitionConfig.getChildren(entryMapping);
            clean(partitionConfig, entryMapping.getDn(), children);

            log.debug("Cleaning tables for "+entryMapping.getDn());
            EntryCache cache = engine.getCache(parentDn, entryMapping);
            cache.clean();
        }
    }

    public void drop() throws Exception {

        for (Iterator i=partitionManager.getConfigs().iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            Collection entryDefinitions = partitionConfig.getRootEntryMappings();
            drop(partitionConfig, null, entryDefinitions);
        }

        //dropMappingsTable();

        connector.drop();
    }

    public void drop(PartitionConfig partitionConfig, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partitionConfig.getChildren(entryMapping);
            drop(partitionConfig, entryMapping.getDn(), children);

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

        Penrose penrose = new Penrose();
        penrose.setHome(homeDirectory);
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
}
