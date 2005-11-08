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
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.schema.*;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineContext;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerContext;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.FilterContext;
import org.safehaus.penrose.acl.ACLEngine;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.*;

/**
 * @author Endi S. Dewata
 */
public class Penrose implements
        AdapterContext,
        EngineContext,
        FilterContext,
        HandlerContext,
        ModuleContext,
        PenroseMBean {
	
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

	private List suffixes = new ArrayList();
	private List normalizedSuffixes = new ArrayList();

	private String trustedKeyStore;

    private String homeDirectory;

	private String rootDn;
	private String rootPassword;

    private ConfigValidator configValidator;
    private Handler handler;
	private ACLEngine aclEngine;
	private FilterTool filterTool;

	private TransformEngine transformEngine;

    private Map connectors = new LinkedHashMap();
	private Map engines = new LinkedHashMap();

    private Map modules = new LinkedHashMap();

    private ServerConfig serverConfig;
    private Map configs = new TreeMap();
    private Schema schema;

    private Logger log = Logger.getLogger(getClass());

	private boolean stopRequested = false;

	private PenroseConnectionPool connectionPool = new PenroseConnectionPool(this);

	public Penrose() {
	}

	public Collection getConfigs() {
		return configs.values();
	}

	/**
	 * Initialize server with a set of suffixes.
	 * 
	 * @param suffixes
	 */
	public void setSuffix(String suffixes[]) {
		log.debug("-------------------------------------------------------------------------------");
		log.debug("Penrose.setSuffix(suffixArray);");

		for (int i = 0; i < suffixes.length; i++) {
			log.debug(" suffix            : " + suffixes[i]);
			this.suffixes.add(suffixes[i]);
		}

		for (int i = 0; i < suffixes.length; i++) {
			this.normalizedSuffixes.add(LDAPDN.normalize(suffixes[i]));
		}

	}

	public int init() throws Exception {

        loadSchema();
        loadServerConfig();
        initServer();

        initConnectors();
        initEngines();

        ConfigReader configReader = new ConfigReader();
        Config config = configReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        addConfig(config);

		return LDAPException.SUCCESS;
	}

    public void loadServerConfig() throws Exception {
        log.debug("-------------------------------------------------------------------------------");
        log.debug("Loading server configurration ...");

        if (trustedKeyStore != null) System.setProperty("javax.net.ssl.trustStore", trustedKeyStore);

        ServerConfigReader reader = new ServerConfigReader();
        serverConfig = reader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");

        if (serverConfig.getRootDn() != null) rootDn = serverConfig.getRootDn();
        if (serverConfig.getRootPassword() != null) rootPassword = serverConfig.getRootPassword();
        //log.debug(serverConfig.toString());
    }

    public void initServer() throws Exception {
        handler = new Handler(this);
        aclEngine = new ACLEngine(this);
        filterTool = new FilterTool(this);
        transformEngine = new TransformEngine(this);

        configValidator = new ConfigValidator();
        configValidator.setServerConfig(serverConfig);
        configValidator.setSchema(schema);
    }

    public void initConnectors() throws Exception {

        for (Iterator i=serverConfig.getConnectorConfigs().iterator(); i.hasNext(); ) {
            ConnectorConfig connectorConfig = (ConnectorConfig)i.next();

            Class clazz = Class.forName(connectorConfig.getConnectorClass());
            Connector connector = (Connector)clazz.newInstance();
            connector.init(serverConfig, connectorConfig);
            connector.start();

            connectors.put(connectorConfig.getConnectorName(), connector);
        }
    }

    public void initEngines() throws Exception {

        for (Iterator i=serverConfig.getEngineConfigs().iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();

            Class clazz = Class.forName(engineConfig.getEngineClass());
            Engine engine = (Engine)clazz.newInstance();
            engine.init(engineConfig, this);

            engines.put(engineConfig.getEngineName(), engine);
        }
    }

	public void addConfig(Config config) throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("Validating config...");

        Collection results = configValidator.validate(config);
        for (Iterator i=results.iterator(); i.hasNext(); ) {
            ConfigValidationResult result = (ConfigValidationResult)i.next();

            if (result.getType().equals(ConfigValidationResult.ERROR)) {
                log.error("ERROR: "+result.getMessage()+" ["+result.getSource()+"]");
            } else {
                log.warn("WARNING: "+result.getMessage()+" ["+result.getSource()+"]");
            }
        }

        log.debug("-------------------------------------------------------------------------------");
        log.debug("Penrose.addConfig(config)");

        log.debug("Registering suffixes:");
        for (Iterator i=config.getRootEntryDefinitions().iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            String ndn = schema.normalize(entryDefinition.getDn());
            log.debug(" - "+ndn);
            configs.put(ndn, config);
        }

        initModules(config);
        getEngine().analyze(config);
        getConnector().addConfig(config);
	}

    public void initModules(Config config) throws Exception {

        for (Iterator i=config.getModuleConfigs().iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();

            Class clazz = Class.forName(moduleConfig.getModuleClass());
            Module module = (Module)clazz.newInstance();
            module.init(moduleConfig);

            modules.put(moduleConfig.getModuleName(), module);
        }
    }

    public void loadSchema() throws Exception {
        SchemaReader reader = new SchemaReader();
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema");
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema"+File.separator+"ext");
        schema = reader.getSchema();
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
        return getEngine("DEFAULT");
    }

    public Engine getEngine(String name) {
        return (Engine)engines.get(name);
    }

    public Connector getConnector() {
        return getConnector("DEFAULT");
    }

    public Connector getConnector(String name) {
        return (Connector)connectors.get(name);
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
    
    public Config getConfig(SourceDefinition sourceDefinition) throws Exception {
        String connectionName = sourceDefinition.getConnectionName();
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

    public Collection getModules(String dn) throws Exception {
        log.debug("Find matching module mapping for "+dn);

        Collection list = new ArrayList();

        Config config = getConfig(dn);
        if (config == null) return list;
        
        for (Iterator i = config.getModuleMappings().iterator(); i.hasNext(); ) {
            Collection c = (Collection)i.next();

            for (Iterator j=c.iterator(); j.hasNext(); ) {
                ModuleMapping moduleMapping = (ModuleMapping)j.next();

                String moduleName = moduleMapping.getModuleName();
                Module module = (Module)modules.get(moduleName);

                if (moduleMapping.match(dn)) {
                    log.debug(" - "+moduleName);
                    list.add(module);
                }
            }
        }

        return list;
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

            Engine engine = getEngine();
            if (engine != null) engine.stop();

            Connector connector = getConnector();
            if (connector != null) connector.stop();

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

    // ------------------------------------------------
    // Getters and Setters
    // ------------------------------------------------
	
    public ACLEngine getACLEngine() {
        return aclEngine;
    }

    public void setACLEngine(ACLEngine aclEngine) {
        this.aclEngine = aclEngine;
    }

    public Collection getEngines() {
        return engines.values();
    }
    public PenroseConnectionPool getConnectionPool() {
        return connectionPool;
    }
    public void setConnectionPool(PenroseConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    public FilterTool getFilterTool() {
        return filterTool;
    }
    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }
    public List getNormalizedSuffixes() {
        return normalizedSuffixes;
    }
    public void setNormalizedSuffixes(List normalizedSuffixes) {
        this.normalizedSuffixes = normalizedSuffixes;
    }
    public String getRootDn() {
        return rootDn;
    }
    public void setRootDn(String rootDn) {
        this.rootDn = rootDn;
    }
    public String getRootPassword() {
        return rootPassword;
    }
    public void setRootPassword(String rootPassword) {
        this.rootPassword = rootPassword;
    }
    public boolean isStopRequested() {
        return stopRequested;
    }
    public void setStopRequested(boolean stopRequested) {
        this.stopRequested = stopRequested;
    }
    public List getSuffixes() {
        return suffixes;
    }
    public void setSuffixes(List suffixes) {
        this.suffixes = suffixes;
    }
    public TransformEngine getTransformEngine() {
        return transformEngine;
    }
    public void setTransformEngine(TransformEngine transformEngine) {
        this.transformEngine = transformEngine;
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

    public Interpreter newInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = serverConfig.getInterpreterConfig("DEFAULT");
        Class clazz = Class.forName(interpreterConfig.getInterpreterClass());
        return (Interpreter)clazz.newInstance();
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

    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    public void setConfigValidator(ConfigValidator configValidator) {
        this.configValidator = configValidator;
    }
}
