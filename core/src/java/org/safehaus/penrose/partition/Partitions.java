package org.safehaus.penrose.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceSyncConfig;
import org.safehaus.penrose.source.SourceSync;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.Engine;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Partitions implements PartitionsMBean {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

    private Map<String,Partition> partitions = new LinkedHashMap<String,Partition>();

    public Partitions() {
    }

    public Partition init(PartitionConfig partitionConfig, PartitionContext partitionContext) throws Exception {

        Partition partition = new Partition(partitionConfig);
        partition.setPartitionContext(partitionContext);

        for (HandlerConfig handlerConfig : partitionConfig.getHandlerConfigs()) {
            Handler handler = partition.createHandler(handlerConfig);
            partition.addHandler(handler);
        }

        for (EngineConfig engineConfig : partitionConfig.getEngineConfigs()) {
            Engine engine = partition.createEngine(engineConfig);
            partition.addEngine(engine);
        }

        for (ConnectionConfig connectionConfig : partitionConfig.getConnectionConfigs().getConnectionConfigs()) {
            if (!connectionConfig.isEnabled()) continue;

            Connection connection = partition.createConnection(connectionConfig);
            partition.addConnection(connection);
        }

        for (SourceConfig sourceConfig : partitionConfig.getSourceConfigs().getSourceConfigs()) {
            if (!sourceConfig.isEnabled()) continue;

            Source source = partition.createSource(sourceConfig);
            partition.addSource(source);
        }

        for (SourceSyncConfig sourceSyncConfig : partitionConfig.getSourceConfigs().getSourceSyncConfigs()) {
            if (!sourceSyncConfig.isEnabled()) continue;

            SourceSync sourceSync = partition.createSourceSync(sourceSyncConfig);
            partition.addSourceSync(sourceSync);
        }

        for (EntryMapping entryMapping : partitionConfig.getDirectoryConfigs().getEntryMappings()) {
            if (!entryMapping.isEnabled()) continue;

            Entry entry = partition.createEntry(entryMapping);
            partition.addEntry(entry);
        }

        for (ModuleConfig moduleConfig : partitionConfig.getModuleConfigs().getModuleConfigs()) {
            if (!moduleConfig.isEnabled()) continue;

            Module module = partition.createModule(moduleConfig);
            partition.addModule(module);
        }

        addPartition(partition);

        return partition;
    }

    public void addPartition(Partition partition) {
        partitions.put(partition.getName(), partition);
    }

    public Partition removePartition(String name) throws Exception {
        return partitions.remove(name);
    }

    public void stop() throws Exception {
        for (Partition partition : partitions.values()) {

            for (Module module : partition.getModules()) {
                module.stop();
            }

            for (SourceSync sourceSync : partition.getSourceSyncs()) {
                sourceSync.stop();
            }

            for (Connection connection : partition.getConnections()) {
                connection.stop();
            }
        }
    }

    public void clear() throws Exception {
        partitions.clear();
    }

    public Partition getPartition(String name) {
        return partitions.get(name);
    }

    public Partition getPartition(SourceMapping sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getSourceConfigs().getSourceConfig(sourceName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getConnectionConfigs().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getConnectionConfigs().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(EntryMapping entryMapping) throws Exception {

        if (entryMapping == null) return null;

        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getDirectoryConfigs().contains(entryMapping)) {
                return partition;
            }
        }

        return null;
    }

    public Partition getPartition(DN dn) throws Exception {

        if (debug) log.debug("Finding partition for \""+dn+"\".");

        if (dn == null) {
            log.debug("DN is null.");
            return null;
        }

        Partition p = null;
        DN s = null;

        for (Partition partition : partitions.values()) {
            if (debug) log.debug("Checking "+partition.getName()+" partition.");

            PartitionConfig partitionConfig = partition.getPartitionConfig();
            Collection<DN> suffixes = partitionConfig.getDirectoryConfigs().getSuffixes();
            for (DN suffix : suffixes) {
                if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                        || dn.endsWith(suffix)) {

                    if (s == null || s.getSize() < suffix.getSize()) {
                        p = partition;
                        s = suffix;
                    }
                }
            }
        }

        if (debug) {
            if (p == null) {
                log.debug("Partition not found.");
            } else {
                log.debug("Found "+p.getName()+" partition.");
            }
        }

        return p;
    }

    public Collection<Partition> getPartitions() {
        return partitions.values();
    }

    public Collection<String> getPartitionNames() {
        return partitions.keySet();
    }

    public Handler getHandler(Partition partition, EntryMapping entryMapping) {
        String handlerName = entryMapping.getHandlerName();
        if (handlerName == null) handlerName = "DEFAULT";

        Handler handler = partition.getHandler(handlerName);
        if (handler != null) {
            if (debug) log.debug("Using "+handlerName+" handler in "+partition.getName()+" partition.");
            return handler;
        }

        if (debug) log.debug("Using "+handlerName+" handler in DEFAULT partition.");
        partition = partitions.get("DEFAULT");
        return partition.getHandler(handlerName);
    }

    public Engine getEngine(Partition partition, Handler handler, EntryMapping entryMapping) {
        String engineName = entryMapping.getEngineName();
        if (engineName == null) engineName = handler.getEngineName();
        if (engineName == null) engineName = "DEFAULT";

        Engine engine = partition.getEngine(engineName);
        if (engine != null) {
            if (debug) log.debug("Using "+engineName+" engine in "+partition.getName()+" partition.");
            return engine;
        }

        if (debug) log.debug("Using "+engineName+" engine in DEFAULT partition.");
        partition = partitions.get("DEFAULT");
        return partition.getEngine(engineName);
    }
}
