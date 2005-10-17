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
import java.lang.reflect.Constructor;

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
import org.safehaus.penrose.cache.*;
import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.acl.ACLEngine;
import org.safehaus.penrose.sync.SyncService;
import org.safehaus.penrose.sync.SyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi S. Dewata
 */
public class Penrose implements
        AdapterContext,
        CacheContext,
        HandlerContext,
        EngineContext,
        SyncContext,
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
    private SyncService syncService;

    private Map entryFilterCaches = new TreeMap();
    private Map entryDataCaches = new TreeMap();
    private Map entrySourceCaches = new TreeMap();

    private Map sourceFilterCaches = new TreeMap();
    private Map sourceDataCaches = new TreeMap();

	private Map engines = new LinkedHashMap();

    private Map connections = new LinkedHashMap();
    private Map modules = new LinkedHashMap();

    private ServerConfig serverConfig;
    private Map configs = new TreeMap();
    private Schema schema;

    private Logger log = LoggerFactory.getLogger(getClass());

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

        initServer();

        ConfigReader reader = new ConfigReader();
        Config config = reader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");
        //log.debug(config.toString());

        addConfig(config);

		return LDAPException.SUCCESS;
	}

    public void initEngine() throws Exception {

        for (Iterator i=serverConfig.getEngineConfigs().iterator(); i.hasNext(); ) {
            EngineConfig engineConfig = (EngineConfig)i.next();

            Class clazz = Class.forName(engineConfig.getEngineClass());
            Engine engine = (Engine)clazz.newInstance();
            engine.init(engineConfig, this);

            engines.put(engineConfig.getEngineName(), engine);
        }
    }

    public Connection getConnection(String name) {
        return (Connection)connections.get(name);
    }

    public void initServer() throws Exception {

        log.debug("-------------------------------------------------------------------------------");
        log.debug("Penrose.initServer()");

        ServerConfigReader reader = new ServerConfigReader();
        reader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");

        serverConfig = reader.getServerConfig();
        if (serverConfig.getRootDn() != null) rootDn = serverConfig.getRootDn();
        if (serverConfig.getRootPassword() != null) rootPassword = serverConfig.getRootPassword();
        //log.debug(serverConfig.toString());

        handler = new Handler(this);
        aclEngine = new ACLEngine(this);
        filterTool = new FilterTool(this);
        transformEngine = new TransformEngine(this);
        syncService = new SyncService(this);

        loadSchema();
        initEngine();

        configValidator = new ConfigValidator();
        configValidator.setServerConfig(serverConfig);
        configValidator.setSchema(schema);

        if (trustedKeyStore != null) System.setProperty("javax.net.ssl.trustStore", trustedKeyStore);
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

        initConnections(config);
        initModules(config);
        getEngine().analyze(config);
	}

    public void initConnections(Config config) throws Exception {
        for (Iterator i = config.getConnectionConfigs().iterator(); i.hasNext();) {
            ConnectionConfig connectionConfig = (ConnectionConfig) i.next();

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null) throw new Exception("Missing adapter name");

            AdapterConfig adapterConfig = serverConfig.getAdapterConfig(adapterName);
            if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName);

            String adapterClass = adapterConfig.getAdapterClass();
            Class clazz = Class.forName(adapterClass);
            Adapter adapter = (Adapter)clazz.newInstance();

            Connection connection = new Connection();
            connection.init(connectionConfig, adapter);

            adapter.init(adapterConfig, this, connection);

            connections.put(connectionConfig.getConnectionName(), connection);
        }

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

    public Engine getEngine() {
        return getEngine("DEFAULT");
    }

    public Engine getEngine(String name) {
        return (Engine)engines.get(name);
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
	public Logger getLog() {
		return log;
	}
	public void setLog(Logger log) {
		this.log = log;
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
        FileInputStream in = new FileInputStream(file);

        byte content[] = new byte[(int)file.length()];
        in.read(content);

        in.close();

		return content;
	}
	
	public void upload(String filename, byte content[]) throws IOException {
		File file = new File((homeDirectory == null ? "" : homeDirectory+File.separator)+filename);
		FileOutputStream out = new FileOutputStream(file);
		out.write(content);
		out.close();
	}

    public Interpreter newInterpreter() throws Exception {
        InterpreterConfig interpreterConfig = serverConfig.getInterpreterConfig("DEFAULT");
        Class clazz = Class.forName(interpreterConfig.getInterpreterClass());
        return (Interpreter)clazz.newInstance();
    }

    public EntryFilterCache getEntryFilterCache(String parentDn, EntryDefinition entryDefinition) throws Exception {
        String cacheName = entryDefinition.getParameter(EntryDefinition.CACHE);
        cacheName = cacheName == null ? EntryDefinition.DEFAULT_CACHE : cacheName;

        String key = entryDefinition.getRdn()+","+parentDn;

        EntryFilterCache entryFilterCache = (EntryFilterCache)entryFilterCaches.get(key);

        if (entryFilterCache == null) {
            entryFilterCache = new EntryFilterCache(this, entryDefinition);
            entryFilterCaches.put(key, entryFilterCache);
        }

        return entryFilterCache;
    }

    public EntrySourceCache getEntrySourceCache(String parentDn, EntryDefinition entryDefinition) throws Exception {
        String cacheName = "EntrySource";

        CacheConfig cacheConfig = serverConfig.getCacheConfig(cacheName);

        String key = entryDefinition.getRdn()+","+parentDn;

        EntrySourceCache entrySourceCache = (EntrySourceCache)entrySourceCaches.get(key);

        if (entrySourceCache == null) {
            entrySourceCache = new EntrySourceCache();
            entrySourceCache.setParentDn(parentDn);
            entrySourceCache.setEntryDefinition(entryDefinition);
            entrySourceCache.init(cacheConfig, this);
            entrySourceCaches.put(key, entrySourceCache);
        }

        return entrySourceCache;
    }

    public EntryDataCache getEntryDataCache(String parentDn, EntryDefinition entryDefinition) throws Exception {
        String cacheName = entryDefinition.getParameter(EntryDefinition.CACHE);
        cacheName = cacheName == null ? EntryDefinition.DEFAULT_CACHE : cacheName;

        CacheConfig cacheConfig = serverConfig.getCacheConfig(cacheName);

        String key = entryDefinition.getRdn()+","+parentDn;

        EntryDataCache cache = (EntryDataCache)entryDataCaches.get(key);

        if (cache == null) {
            String cacheClass = cacheConfig.getCacheClass();
            cacheClass = cacheClass == null ? CacheConfig.DEFAULT_ENTRY_DATA_CACHE : cacheClass;

            Class clazz = Class.forName(cacheClass);
            cache = (EntryDataCache)clazz.newInstance();
            cache.setParentDn(parentDn);
            cache.setEntryDefinition(entryDefinition);
            cache.init(cacheConfig, this);

            entryDataCaches.put(key, cache);
        }

        return cache;
    }

    public SourceFilterCache getSourceFilterCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String cacheName = sourceDefinition.getParameter(SourceDefinition.CACHE);
        cacheName = cacheName == null ? SourceDefinition.DEFAULT_CACHE : cacheName;

        CacheConfig cacheConfig = serverConfig.getCacheConfig(cacheName);

        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        SourceFilterCache cache = (SourceFilterCache)sourceFilterCaches.get(key);
        if (cache == null) {
            cache = new SourceFilterCache(this, sourceDefinition);
            sourceFilterCaches.put(key, cache);
        }
        return cache;
    }

    public SourceDataCache getSourceDataCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String cacheName = sourceDefinition.getParameter(SourceDefinition.CACHE);
        cacheName = cacheName == null ? SourceDefinition.DEFAULT_CACHE : cacheName;

        CacheConfig cacheConfig = serverConfig.getCacheConfig(cacheName);

        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        SourceDataCache cache = (SourceDataCache)sourceDataCaches.get(key);

        if (cache == null) {
            String cacheClass = cacheConfig.getCacheClass();
            cacheClass = cacheClass == null ? CacheConfig.DEFAULT_SOURCE_DATA_CACHE : cacheClass;

            Class clazz = Class.forName(cacheClass);
            cache = (SourceDataCache)clazz.newInstance();
            cache.setSourceDefinition(sourceDefinition);
            cache.init(cacheConfig, this);

            sourceDataCaches.put(key, cache);
        }

        return cache;
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

    public SyncService getSyncService() {
        return syncService;
    }

    public void setSyncService(SyncService syncService) {
        this.syncService = syncService;
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