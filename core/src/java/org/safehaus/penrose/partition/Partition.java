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
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.directory.Entry;

/**
 * @author Endi S. Dewata
 */
public class Partition implements PartitionMBean, Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static Collection<String> EMPTY_STRINGS = new ArrayList<String>();
    public final static Collection<Source> EMPTY_SOURCES = new ArrayList<Source>();
    public final static Collection<SourceRef> EMPTY_SOURCEREFS = new ArrayList<SourceRef>();

    private PartitionConfig partitionConfig;
    private PenroseConfig   penroseConfig;
    private PenroseContext  penroseContext;

    private ClassLoader classLoader;

    private Map<String,Connection> connections = new LinkedHashMap<String,Connection>();
    private Map<String,Source>     sources     = new LinkedHashMap<String,Source>();
    private Map<String,SourceSync> sourceSyncs = new LinkedHashMap<String,SourceSync>();
    private Map<String,Entry>      entries     = new LinkedHashMap<String,Entry>();
    private Map<String,Module>     modules     = new LinkedHashMap<String,Module>();

    public Map<String,Map<String, SourceRef>> sourceRefs        = new LinkedHashMap<String,Map<String,SourceRef>>();
    public Map<String,Map<String,SourceRef>>  primarySourceRefs = new LinkedHashMap<String,Map<String,SourceRef>>();

    public Partition(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;

        Collection<URL> classPaths = partitionConfig.getClassPaths();
        classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]));
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

    public boolean isEnabled() {
        return partitionConfig.isEnabled();
    }

    public String getHandlerName() {
        return partitionConfig.getHandlerName();
    }

    public String getEngineName() {
        return partitionConfig.getEngineName();
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

        partition.classLoader = classLoader;

        return partition;
    }

    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name.");

        AdapterConfig adapterConfig = partitionConfig.getConnectionConfigs().getAdapterConfig(adapterName);

        if (adapterConfig == null) {
            adapterConfig = penroseConfig.getAdapterConfig(adapterName);
        }

        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName+".");

        Connection connection = new Connection(this, connectionConfig, adapterConfig);
        connection.setPenroseConfig(penroseConfig);
        connection.setPenroseContext(penroseContext);
        connection.init();
        connection.start();

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

        Source source = new Source(this, sourceConfig);
        source.setConnection(connection);

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

        module.setModuleConfig(moduleConfig);
        module.setPartition(this);
        module.setPenroseConfig(penroseConfig);
        module.setPenroseContext(penroseContext);
        module.init();
        module.start();

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

        Connection connection = getConnection(sourceConfig.getConnectionName());
        Adapter adapter = connection.getAdapter();
        String syncClassName = adapter.getSyncClassName();
        Class clazz = classLoader.loadClass(syncClassName);

        SourceSync sourceSync = (SourceSync)clazz.newInstance();

        sourceSync.setSourceSyncConfig(sourceSyncConfig);
        sourceSync.setPartition(this);
        sourceSync.setPenroseConfig(penroseConfig);
        sourceSync.setPenroseContext(penroseContext);
        sourceSync.init();
        sourceSync.start();

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

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
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
}
