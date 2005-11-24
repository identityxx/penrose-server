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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ietf.ldap.*;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.module.ModuleContext;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.*;

/**
 * @author Endi S. Dewata
 */
public class Penrose implements
        AdapterContext,
        ModuleContext,
        PenroseMBean {
	
    Logger log = Logger.getLogger(Penrose.class);

	// ------------------------------------------------
	// Constants
	// ------------------------------------------------
    public final static String ENGINE_LOGGER     = "org.safehaus.penrose.engine";
    public final static String CONFIG_LOGGER     = "org.safehaus.penrose.config";
    public final static String SCHEMA_LOGGER     = "org.safehaus.penrose.schema";
    public final static String FILTER_LOGGER     = "org.safehaus.penrose.filter";
    public final static String ADAPTER_LOGGER    = "org.safehaus.penrose.adapter";
    public final static String PASSWORD_LOGGER   = "org.safehaus.penrose.password";
    public final static String CACHE_LOGGER      = "org.safehaus.penrose.cache";
    public final static String CONNECTION_LOGGER = "org.safehaus.penrose.connection";
    public final static String TRANSFORM_LOGGER  = "org.safehaus.penrose.transform";
    public final static String MODULE_LOGGER     = "org.safehaus.penrose.module";
    public final static String SECURITY_LOGGER   = "org.safehaus.penrose.security";

    public final static String SEARCH_LOGGER     = "org.safehaus.penrose.search";
    public final static String BIND_LOGGER       = "org.safehaus.penrose.bind";
    public final static String ADD_LOGGER        = "org.safehaus.penrose.add";
    public final static String MODIFY_LOGGER     = "org.safehaus.penrose.modify";
    public final static String DELETE_LOGGER     = "org.safehaus.penrose.delete";
    public final static String COMPARE_LOGGER    = "org.safehaus.penrose.compare";
    public final static String MODRDN_LOGGER     = "org.safehaus.penrose.modrdn";

    public final static String PENROSE_HOME      = "org.safehaus.penrose.home";

	public final static boolean SEARCH_IN_BACKGROUND = true;
	public final static int WAIT_TIMEOUT = 10000; // wait timeout is 10 seconds

	private String trustedKeyStore;

    private String homeDirectory;

    private Connector connector;
	private Engine engine;
    private Handler handler;

    private ServerConfig serverConfig;
    private Schema schema;

    private Map configs = new TreeMap();
    public ConnectionManager connectionManager;

    private InterpreterFactory interpreterFactory;

	private boolean stopRequested = false;

	private PenroseConnectionPool connectionPool = new PenroseConnectionPool(this);

	public Penrose() {
	}

	public Collection getConfigs() {
		return configs.values();
	}

	public int start() throws Exception {

        loadServerConfig();
        loadSchema();
        loadConfigs();

        initInterpreter();
        initConnections();
        initConnectors();
        initEngines();
        initHandler();

		return LDAPException.SUCCESS;
	}

    public void loadServerConfig() throws Exception {
        //log.debug("-------------------------------------------------------------------------------");
        //log.debug("Loading server configurration ...");

        if (trustedKeyStore != null) System.setProperty("javax.net.ssl.trustStore", trustedKeyStore);

        ServerConfigReader reader = new ServerConfigReader();
        serverConfig = reader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");
    }

    public void loadSchema() throws Exception {
        SchemaReader reader = new SchemaReader();
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema");
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema"+File.separator+"ext");
        schema = reader.getSchema();
    }

    public void loadConfigs() throws Exception {

        ConfigValidator configValidator = new ConfigValidator();
        configValidator.setServerConfig(serverConfig);
        configValidator.setSchema(schema);

        ConfigReader configReader = new ConfigReader();
        Config config = configReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        for (Iterator i=config.getRootEntryDefinitions().iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            String ndn = schema.normalize(entryDefinition.getDn());
            configs.put(ndn, config);
        }

        File partitions = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+"partitions");
        if (partitions.exists()) {
            File files[] = partitions.listFiles();
            for (int i=0; i<files.length; i++) {
                File partition = files[i];

                config = configReader.read(partition.getAbsolutePath());

                for (Iterator j=config.getRootEntryDefinitions().iterator(); j.hasNext(); ) {
                    EntryDefinition entryDefinition = (EntryDefinition)j.next();
                    String ndn = schema.normalize(entryDefinition.getDn());
                    configs.put(ndn, config);
                }
            }
        }

        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            config = (Config)i.next();

            Collection results = configValidator.validate(config);

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                ConfigValidationResult result = (ConfigValidationResult)j.next();

                if (result.getType().equals(ConfigValidationResult.ERROR)) {
                    log.error("ERROR: "+result.getMessage()+" ["+result.getSource()+"]");
                } else {
                    log.warn("WARNING: "+result.getMessage()+" ["+result.getSource()+"]");
                }
            }

        }
    }

    public void initInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = serverConfig.getInterpreterConfig();
        interpreterFactory = new InterpreterFactory(interpreterConfig);
    }

    public void initConnections() throws Exception {
        connectionManager = new ConnectionManager();

        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
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

        for (Iterator j=configs.values().iterator(); j.hasNext(); ) {
            Config config = (Config)j.next();
            connector.addConfig(config);
        }

        connector.start();
    }

    public void initEngines() throws Exception {

        EngineConfig engineConfig = serverConfig.getEngineConfig();

        Class clazz = Class.forName(engineConfig.getEngineClass());
        engine = (Engine)clazz.newInstance();

        engine.setServerConfig(serverConfig);
        engine.setSchema(schema);
        engine.setInterpreterFactory(interpreterFactory);
        engine.setConnector(getConnector());
        engine.setConnectionManager(connectionManager);
        engine.init(engineConfig);

        for (Iterator j=configs.values().iterator(); j.hasNext(); ) {
            Config config = (Config)j.next();
            engine.addConfig(config);
        }

        engine.start();
    }

    public void initHandler() throws Exception {
        handler = new Handler();
        handler.setSchema(schema);
        handler.setInterpreterFactory(interpreterFactory);
        handler.setEngine(getEngine());
        handler.setRootDn(serverConfig.getRootDn());
        handler.setRootPassword(serverConfig.getRootPassword());

        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            handler.addConfig(config);
        }

        handler.init();
    }

    public Collection listFiles(String directory) throws Exception {
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+directory);
        File children[] = file.listFiles();
        Collection result = new ArrayList();
        for (int i=0; i<children.length; i++) {
            if (children[i].isDirectory()) {
                result.addAll(listFiles(directory+File.separator+children[i].getName()));
            } else {
                result.add(directory+File.separator+children[i].getName());
            }
        }
        return result;
    }

    public Collection getLoggerNames(String path) throws Exception {
        log.debug("Loggers under "+path);
        Collection loggerNames = new TreeSet();

        Enumeration e = LogManager.getCurrentLoggers();
        while (e.hasMoreElements()) {
    		Logger logger = (Logger)e.nextElement();
    		log.debug(" - "+logger.getName()+": "+logger.getEffectiveLevel());
            loggerNames.add(logger.getName());
    	}

        return loggerNames;
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

    public Config getConfig(Source source) throws Exception {
        String connectionName = source.getConnectionName();
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            if (config.getConnectionConfig(connectionName) != null) return config;
        }
        return null;
    }
    
    public Config getConfig(String dn) throws Exception {
        String ndn = schema.normalize(dn);
        for (Iterator i=configs.keySet().iterator(); i.hasNext(); ) {
            String suffix = (String)i.next();
            if (ndn.endsWith(suffix)) return (Config)configs.get(suffix);
        }
        return null;
    }

    public SearchResults search(String base, int scope,
            int deref, String filter, Collection attributeNames)
            throws Exception {

        return getHandler().search(null, base, scope, deref, filter, attributeNames);
    }

    public int add(LDAPEntry entry) throws Exception {
        return getHandler().add(null, entry);
    }

    public int delete(String dn) throws Exception {
        return getHandler().delete(null, dn);
    }

    public int modify(String dn, List modifications) throws Exception {
        return getHandler().modify(null, dn, modifications);
    }

    public int modrdn(String dn, String newRdn) throws Exception {
        return getHandler().modrdn(null, dn, newRdn);
    }

	public int compare(String dn, String attributeName,
			String attributeValue) throws Exception {
        return getHandler().compare(null, dn, attributeName, attributeValue);
	}

	/**
	 * Convert entry to string.
	 * 
	 * @param entry Entry.
	 * @return LDAP entry in LDIF format
	 * @throws Exception
	 */
	public String toString(LDAPEntry entry) throws Exception {
        return Entry.toString(entry);
	}

	/**
	 * Shutdown gracefully
	 */
	public void stop() {
        
        if (stopRequested) return;

        try {
            log.debug("Stop requested...");
            stopRequested = true;

            engine.stop();
            connector.stop();

            // close all the pools, including all the connections
            //connectionPool.closeAll();

            log.warn("Penrose has been shutdown.");

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            //System.exit(0);
        }
	}

    public PenroseConnection openConnection() throws Exception {
        return connectionPool.createConnection();
    }

    public void removeConnection(PenroseConnection connection) {
        connectionPool.removeConnection(connection);
    }

    // ------------------------------------------------
    // Listeners
    // ------------------------------------------------

    public void addConnectionListener(ConnectionListener l) {
    }

    public void removeConnectionListener(ConnectionListener l) {
    }

    public void addBindListener(BindListener l) {
    }

    public void removeBindListener(BindListener l) {
    }

    public void addSearchListener(SearchListener l) {
    }

    public void removeSearchListener(SearchListener l) {
    }

    public void addCompareListener(CompareListener l) {
    }

    public void removeCompareListener(CompareListener l) {
    }

    public void addAddListener(AddListener l) {
    }

    public void removeAddListener(AddListener l) {
    }

    public void addDeleteListener(DeleteListener l) {
    }

    public void removeDeleteListener(DeleteListener l) {
    }

    public void addModifyListener(ModifyListener l) {
    }

    public void removeModifyListener(ModifyListener l) {
    }

    public PenroseConnectionPool getConnectionPool() {
        return connectionPool;
    }
    public void setConnectionPool(PenroseConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }
    public boolean isStopRequested() {
        return stopRequested;
    }
    public void setStopRequested(boolean stopRequested) {
        this.stopRequested = stopRequested;
    }
    public String getTrustedKeyStore() {
        return trustedKeyStore;
    }
    public void setTrustedKeyStore(String trustedKeyStore) {
        this.trustedKeyStore = trustedKeyStore;
    }

    public byte[] download(String filename) throws IOException {
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
        log.debug("Downloading "+file.getAbsolutePath());

        FileInputStream in = new FileInputStream(file);

        byte content[] = new byte[(int)file.length()];
        in.read(content);

        in.close();

        return content;
    }
	
    public void upload(String filename, byte content[]) throws IOException {
        File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
        log.debug("Uploading "+file.getAbsolutePath());
        FileOutputStream out = new FileOutputStream(file);
        out.write(content);
        out.close();
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
}
