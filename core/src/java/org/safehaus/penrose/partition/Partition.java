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
package org.safehaus.penrose.partition;

import java.util.*;
import java.net.URL;
import java.net.URLClassLoader;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionContext;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleContext;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineConfig;

/**
 * @author Endi S. Dewata
 */
public class Partition implements PartitionMBean, Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static Collection<String> EMPTY_STRINGS = new ArrayList<String>();
    public final static Collection<Source> EMPTY_SOURCES = new ArrayList<Source>();
    public final static Collection<SourceRef> EMPTY_SOURCEREFS = new ArrayList<SourceRef>();

    protected PartitionConfig partitionConfig;
    protected PartitionContext partitionContext;

    protected ClassLoader classLoader;

    protected Map<String, Handler> handlers = new LinkedHashMap<String,Handler>();
    protected Map<String, Engine>  engines  = new LinkedHashMap<String,Engine>();


    protected Map<String,Connection> connections = new LinkedHashMap<String,Connection>();
    protected Map<String,Source>     sources     = new LinkedHashMap<String,Source>();
    protected Map<String,SourceSync> sourceSyncs = new LinkedHashMap<String,SourceSync>();
    protected Map<String,Entry>      entries     = new LinkedHashMap<String,Entry>();
    protected Map<String,Module>     modules     = new LinkedHashMap<String,Module>();

    protected Map<String,Map<String,SourceRef>> sourceRefs        = new LinkedHashMap<String,Map<String,SourceRef>>();
    protected Map<String,Map<String,SourceRef>> primarySourceRefs = new LinkedHashMap<String,Map<String,SourceRef>>();

    public Partition() {
    }

    public void init(PartitionConfig partitionConfig, PartitionContext partitionContext) throws Exception {

        log.debug("Initializing "+partitionConfig.getName()+" partition.");

        this.partitionConfig = partitionConfig;
        this.partitionContext = partitionContext;

        Collection<URL> classPaths = partitionConfig.getClassPaths();
        classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());

        for (HandlerConfig handlerConfig : partitionConfig.getHandlerConfigs()) {
            Handler handler = createHandler(handlerConfig);
            addHandler(handler);
        }

        for (EngineConfig engineConfig : partitionConfig.getEngineConfigs()) {
            Engine engine = createEngine(engineConfig);
            addEngine(engine);
        }

        for (ConnectionConfig connectionConfig : partitionConfig.getConnectionConfigs().getConnectionConfigs()) {
            if (!connectionConfig.isEnabled()) continue;

            Connection connection = createConnection(connectionConfig);
            addConnection(connection);
        }

        for (SourceConfig sourceConfig : partitionConfig.getSourceConfigs().getSourceConfigs()) {
            if (!sourceConfig.isEnabled()) continue;

            Source source = createSource(sourceConfig);
            addSource(source);
        }

        for (SourceSyncConfig sourceSyncConfig : partitionConfig.getSourceConfigs().getSourceSyncConfigs()) {
            if (!sourceSyncConfig.isEnabled()) continue;

            SourceSync sourceSync = createSourceSync(sourceSyncConfig);
            addSourceSync(sourceSync);
        }

        for (EntryMapping entryMapping : partitionConfig.getDirectoryConfigs().getEntryMappings()) {
            if (!entryMapping.isEnabled()) continue;

            Entry entry = createEntry(entryMapping);
            addEntry(entry);
        }

        for (ModuleConfig moduleConfig : partitionConfig.getModuleConfigs().getModuleConfigs()) {
            if (!moduleConfig.isEnabled()) continue;

            Module module = createModule(moduleConfig);
            addModule(module);
        }

        log.debug("Partition "+partitionConfig.getName()+" started.");
    }

    public void destroy() throws Exception {
        log.debug("Stopping "+partitionConfig.getName()+" partition.");

        for (Module module : modules.values()) {
            module.destroy();
        }

        for (SourceSync sourceSync : sourceSyncs.values()) {
            sourceSync.destroy();
        }

        for (Source source : sources.values()) {
            source.destroy();
        }

        for (Connection connection : connections.values()) {
            connection.destroy();
        }

        log.debug("Partition "+partitionConfig.getName()+" stopped.");
    }

    public String getName() {
        return partitionConfig.getName();
    }

    public void setName(String name) {
        partitionConfig.setName(name);
    }

    public String getDescription() {
        return partitionConfig.getDescription();
    }

    public void setDescription(String description) {
        partitionConfig.setDescription(description);
    }

    public Map<String,String> getParameters() {
        return partitionConfig.getParameters();
    }

    public Collection<String> getParameterNames() {
        return partitionConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return partitionConfig.getParameter(name);
    }

    public boolean isEnabled() {
        return partitionConfig.isEnabled();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public String toString() {
        return partitionConfig.getName();
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public Object clone() throws CloneNotSupportedException {
        Partition partition = (Partition)super.clone();

        partition.partitionConfig = (PartitionConfig)partitionConfig.clone();
        partition.partitionContext = (PartitionContext)partitionContext.clone();

        partition.classLoader = classLoader;

        return partition;
    }

    public Handler createHandler(HandlerConfig handlerConfig) throws Exception {

        String handlerName = handlerConfig.getName();
        if (handlerName == null) throw new Exception("Missing handler name.");

        log.debug("Initializing handler "+handlerName+".");

        String handlerClass = handlerConfig.getHandlerClass();
        Class clazz = classLoader.loadClass(handlerClass);
        Handler handler = (Handler)clazz.newInstance();

        handler.setPartition(this);
        handler.setPenroseContext(partitionContext.getPenroseContext());
        handler.init(handlerConfig);

        return handler;
    }

    public void addHandler(Handler handler) {
        handlers.put(handler.getName(), handler);
    }

    public Handler getHandler(String name) {
        return handlers.get(name);
    }

    public Handler getHandler(Partition partition, EntryMapping entryMapping) {
        String handlerName = entryMapping.getHandlerName();
        if (handlerName != null) return handlers.get(handlerName);

        return handlers.get("DEFAULT");
    }

    public Engine createEngine(EngineConfig engineConfig) throws Exception {

        String engineName = engineConfig.getName();
        if (engineName == null) throw new Exception("Missing engine name.");

        log.debug("Initializing engine "+engineName+".");

        String engineClass = engineConfig.getEngineClass();
        Class clazz = classLoader.loadClass(engineClass);
        Engine engine = (Engine)clazz.newInstance();

        engine.setPartition(this);
        engine.setPenroseContext(partitionContext.getPenroseContext());
        engine.init(engineConfig);

        return engine;
    }

    public void addEngine(Engine engine) {
        engines.put(engine.getName(), engine);
    }

    public Engine getEngine(String name) {
        return engines.get(name);
    }

    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name.");

        AdapterConfig adapterConfig = partitionConfig.getAdapterConfig(adapterName);

        if (adapterConfig == null) {
            adapterConfig = partitionContext.getPenroseConfig().getAdapterConfig(adapterName);
        }

        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName+".");

        ConnectionContext connectionContext = new ConnectionContext();
        connectionContext.setPartition(this);

        Connection connection = new Connection();
        connection.init(connectionConfig, connectionContext, adapterConfig);

        return connection;
    }

    public void addConnection(Connection connection) {
        connections.put(connection.getName(), connection);
    }

    public Collection<Connection> getConnections() {
        return connections.values();
    }

    public Connection getConnection(String name) {
        return connections.get(name);
    }

    public Source createSource(
            SourceConfig sourceConfig
    ) throws Exception {
        Connection connection = getConnection(sourceConfig.getConnectionName());
        if (connection == null) throw new Exception("Unknown connection "+sourceConfig.getConnectionName()+".");

        return createSource(sourceConfig, connection);
    }

    public Source createSource(
            SourceConfig sourceConfig,
            Connection connection
    ) throws Exception {

        log.debug("Initializing source "+sourceConfig.getName()+".");

        SourceContext sourceContext = new SourceContext();
        sourceContext.setPartition(this);
        sourceContext.setConnection(connection);

        Source source = new Source();
        source.init(sourceConfig, sourceContext);

        return source;
    }

    public void addSource(Source source) {
        sources.put(source.getName(), source);
    }

    public Collection<Source> getSources() {
        return sources.values();
    }

    public Source getSource(String name) {
        return sources.get(name);
    }

    public void addSourceSync(SourceSync sourceSync) {
        sourceSyncs.put(sourceSync.getName(), sourceSync);
    }

    public Collection<SourceSync> getSourceSyncs() {
        return sourceSyncs.values();
    }

    public SourceSync getSourceSync(String name) {
        return sourceSyncs.get(name);
    }

    public Module createModule(ModuleConfig moduleConfig) throws Exception {

        if (debug) log.debug("Initializing module "+moduleConfig.getName()+".");

        String moduleClass = moduleConfig.getModuleClass();
        Class clazz = classLoader.loadClass(moduleClass);
        Module module = (Module)clazz.newInstance();

        ModuleContext moduleContext = new ModuleContext();
        moduleContext.setPartition(this);

        module.init(moduleConfig, moduleContext);

        return module;
    }

    public void addModule(Module module) {
        modules.put(module.getName(), module);
    }

    public Collection<Module> getModules() {
        return modules.values();
    }

    public Module getModule(String name) {
        return modules.get(name);
    }

    public Collection<Module> getModules(DN dn) throws Exception {

        if (debug) log.debug("Modules:");

        Collection<Module> list = new ArrayList<Module>();

        for (Collection<ModuleMapping> moduleMappings : partitionConfig.getModuleConfigs().getModuleMappings()) {

            for (ModuleMapping moduleMapping : moduleMappings) {
                String moduleName = moduleMapping.getModuleName();

                boolean b = moduleMapping.match(dn);
                if (debug) log.debug(" - "+moduleName+": "+b);

                if (!b) continue;

                Module module = getModule(moduleName);
                list.add(module);
            }
        }

        return list;
    }

    public SourceSync createSourceSync(SourceSyncConfig sourceSyncConfig) throws Exception {

        log.debug("Initializing source sync "+sourceSyncConfig.getName()+".");

        SourceConfig sourceConfig = sourceSyncConfig.getSourceConfig();
        String changeLogName = sourceSyncConfig.getParameter(SourceSync.CHANGELOG);

        Connection connection = getConnection(sourceConfig.getConnectionName());
        Adapter adapter = connection.getAdapter();
        String syncClassName = adapter.getSyncClassName();
        Class clazz = classLoader.loadClass(syncClassName);

        SourceSync sourceSync = (SourceSync)clazz.newInstance();

        SourceSyncContext sourceSyncContext = new SourceSyncContext();
        sourceSyncContext.setPartition(this);
        
        sourceSync.init(sourceSyncConfig, sourceSyncContext);

        return sourceSync;
    }

    public Entry createEntry(EntryMapping entryMapping) throws Exception {

        log.debug("Initializing entry mapping "+entryMapping.getDn()+".");

        Entry entry = new Entry(entryMapping);

        for (SourceMapping sourceMapping : entryMapping.getSourceMappings()) {
            SourceRef sourceRef = createSourceRef(entryMapping, sourceMapping);
            addSourceRef(entryMapping, sourceRef);
        }

        EntryMapping em = entryMapping;

        while (em != null) {

            String primarySourceName = null;
            Collection<AttributeMapping> rdnAttributeMappings = em.getRdnAttributeMappings();
            for (AttributeMapping rdnAttributeMapping : rdnAttributeMappings) {
                String variable = rdnAttributeMapping.getVariable();
                if (variable == null) continue;

                int i = variable.indexOf('.');
                if (i < 0) continue;

                primarySourceName = variable.substring(0, i);
                break;
            }

            if (primarySourceName != null) {
                SourceRef primarySourceRef = getSourceRef(em, primarySourceName);
                if (primarySourceRef == null) throw new Exception("Unknown source "+primarySourceName);

                addPrimarySourceRef(entryMapping, primarySourceRef);
            }

            em = partitionConfig.getDirectoryConfigs().getParent(em);
        }

        return entry;
    }

    public SourceRef createSourceRef(EntryMapping entryMapping, SourceMapping sourceMapping) throws Exception {

        log.debug("Initializing source mapping "+sourceMapping.getName()+".");

        Source source = getSource(sourceMapping.getSourceName());
        if (source == null) throw new Exception("Unknown source "+sourceMapping.getSourceName()+".");

        return new SourceRef(source, sourceMapping);
    }

    public void addSourceRef(EntryMapping entryMapping, SourceRef sourceRef) {
        Map<String,SourceRef> map = sourceRefs.get(entryMapping.getId());
        if (map == null) {
            map = new LinkedHashMap<String,SourceRef>();
            sourceRefs.put(entryMapping.getId(), map);
        }

        map.put(sourceRef.getAlias(), sourceRef);
    }

    public void addPrimarySourceRef(EntryMapping entryMapping, SourceRef sourceRef) {

        Map<String,SourceRef> primaryMap = primarySourceRefs.get(entryMapping.getId());
        if (primaryMap == null) {
            primaryMap = new LinkedHashMap<String,SourceRef>();
            primarySourceRefs.put(entryMapping.getId(), primaryMap);
        }

        primaryMap.put(sourceRef.getAlias(), sourceRef);
    }

    public Collection<SourceRef> getPrimarySourceRefs(EntryMapping entryMapping) {

        Map<String,SourceRef> primaryMap = primarySourceRefs.get(entryMapping.getId());
        if (primaryMap == null) return EMPTY_SOURCEREFS;

        return primaryMap.values();
    }

    public Collection<String> getSourceRefNames(EntryMapping entryMapping) {

        Map<String,SourceRef> map = sourceRefs.get(entryMapping.getId());
        if (map == null) return EMPTY_STRINGS;

        return new ArrayList<String>(map.keySet()); // return Serializable list
    }

    public Collection<SourceRef> getSourceRefs(EntryMapping entryMapping) {

        Map<String,SourceRef> map = sourceRefs.get(entryMapping.getId());
        if (map == null) return EMPTY_SOURCEREFS;

        return map.values();
    }

    public SourceRef getSourceRef(EntryMapping entryMapping, String sourceName) {

        Map<String,SourceRef> map = sourceRefs.get(entryMapping.getId());
        if (map == null) return null;

        return map.get(sourceName);
    }

    public void addEntry(Entry entry) {
        entries.put(entry.getId(), entry);
    }

    public Collection<Entry> getEntries() {
        return entries.values();
    }

    public Entry getEntry(String id) {
        return entries.get(id);
    }

    public PartitionContext getPartitionContext() {
        return partitionContext;
    }

    public void setPartitionContext(PartitionContext partitionContext) {
        this.partitionContext = partitionContext;
    }
}
