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
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.cache.EntryCache;
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

    private String homeDirectory;

    private ServerConfig serverConfig;
    private Schema schema;

    private ConfigManager configManager;
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
        setSystemProperties();

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
        ServerConfigReader reader = new ServerConfigReader();
        serverConfig = reader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");
    }

    public void setSystemProperties() throws Exception {
        for (Iterator i=serverConfig.getSystemPropertyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = serverConfig.getSystemProperty(name);

            System.setProperty(name, value);
        }
    }

    public void loadSchema() throws Exception {
        SchemaReader reader = new SchemaReader();
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema");
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema"+File.separator+"ext");
        schema = reader.getSchema();
    }

    public void loadConfigs() throws Exception {

        configManager = new ConfigManager();
        configManager.setServerConfig(serverConfig);
        configManager.setSchema(schema);
        configManager.init();

        configManager.load((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        File partitions = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"partitions");
        if (partitions.exists()) {
            File files[] = partitions.listFiles();
            for (int i=0; i<files.length; i++) {
                File partition = files[i];

                configManager.load(partition.getAbsolutePath());
            }
        }
    }

    public void initInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = serverConfig.getInterpreterConfig();
        interpreterFactory = new InterpreterFactory(interpreterConfig);
    }

    public void initConnections() throws Exception {
        connectionManager = new ConnectionManager();

        for (Iterator i=configManager.getConfigs().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();

            Collection connectionConfigs = config.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();
                connectionManager.addConnectionConfig(connectionConfig);
            }
        }

        connectionManager.init();
    }

    public void initConnectors() throws Exception {

        ConnectorConfig connectorConfig = serverConfig.getConnectorConfig();

        Class clazz = Class.forName(connectorConfig.getConnectorClass());
        connector = (Connector)clazz.newInstance();

        connector.setServerConfig(serverConfig);
        connector.setConnectionManager(connectionManager);
        connector.init(connectorConfig);
        connector.setConfigManager(configManager);

        connector.start();
    }

    public void initEngines() throws Exception {

        EngineConfig engineConfig = serverConfig.getEngineConfig();

        Class clazz = Class.forName(engineConfig.getEngineClass());
        engine = (Engine)clazz.newInstance();

        engine.setServerConfig(serverConfig);
        engine.setSchema(schema);
        engine.setInterpreterFactory(interpreterFactory);
        engine.setConnector(connector);
        engine.setConnectionManager(connectionManager);
        engine.init(engineConfig);
        engine.setConfigManager(configManager);

        engine.start();
    }

    public void initHandler() throws Exception {
        handler = new Handler();
        handler.setSchema(schema);
        handler.setInterpreterFactory(interpreterFactory);
        handler.setEngine(engine);
        handler.setRootDn(serverConfig.getRootDn());
        handler.setRootPassword(serverConfig.getRootPassword());
        handler.setConfigManager(configManager);

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

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public String getHomeDirectory() {
        return homeDirectory;
    }

    public void setHomeDirectory(String homeDirectory) {
        this.homeDirectory = homeDirectory;
    }

    public InterpreterFactory getInterpreterFactory() {
        return interpreterFactory;
    }

    public void setInterpreterFactory(InterpreterFactory interpreterFactory) {
        this.interpreterFactory = interpreterFactory;
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public void setConfigManager(ConfigManager configManager) {
        this.configManager = configManager;
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

        for (Iterator i=configManager.getConfigs().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            create(config, null, entryDefinitions);
        }
    }

    public void create(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            log.debug("Creating tables for "+entryDefinition.getDn());
            EntryCache cache = engine.getCache(parentDn, entryDefinition);
            cache.create();

            Collection children = config.getChildren(entryDefinition);
            create(config, entryDefinition.getDn(), children);
        }
    }

    public void load() throws Exception {
        connector.load();

        for (Iterator i=configManager.getConfigs().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            load(config);
        }
    }

    public void load(Config config) throws Exception {
        Collection entryDefinitions = config.getRootEntryDefinitions();
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            log.debug("Loading entries under "+entryDefinition.getDn());

            SearchResults sr = handler.search(
                    null,
                    entryDefinition.getDn(),
                    LDAPConnection.SCOPE_SUB,
                    LDAPSearchConstraints.DEREF_NEVER,
                    "(objectClass=*)",
                    new ArrayList()
            );

            while (sr.hasNext()) sr.next();
        }
    }

    public void clean() throws Exception {

        for (Iterator i=configManager.getConfigs().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            clean(config, null, entryDefinitions);
        }

        connector.clean();
    }

    public void clean(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            Collection children = config.getChildren(entryDefinition);
            clean(config, entryDefinition.getDn(), children);

            log.debug("Cleaning tables for "+entryDefinition.getDn());
            EntryCache cache = engine.getCache(parentDn, entryDefinition);
            cache.clean();
        }
    }

    public void drop() throws Exception {

        for (Iterator i=configManager.getConfigs().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            drop(config, null, entryDefinitions);
        }

        //dropMappingsTable();

        connector.drop();
    }

    public void drop(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            Collection children = config.getChildren(entryDefinition);
            drop(config, entryDefinition.getDn(), children);

            log.debug("Deleting entries under "+entryDefinition.getDn());
            EntryCache cache = engine.getCache(parentDn, entryDefinition);
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

        String home = System.getProperty("penrose.home");

        String command = args[0];

        Penrose penrose = new Penrose();
        penrose.setHomeDirectory(home);
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
