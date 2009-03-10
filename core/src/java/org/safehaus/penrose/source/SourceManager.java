package org.safehaus.penrose.source;

import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class SourceManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    public Partition partition;
    protected SourceConfigManager sourceConfigManager;

    protected Map<String,Source> sources                             = new LinkedHashMap<String,Source>();
    protected Map<String,Collection<Source>> sourcesByConnectionName = new LinkedHashMap<String,Collection<Source>>();

    public SourceManager(Partition partition) {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        sourceConfigManager = partitionConfig.getSourceConfigManager();
    }

    public void init() throws Exception {

        Collection<String> names = new ArrayList<String>();
        names.addAll(getSourceNames());

        for (String sourceName : names) {

            SourceConfig sourceConfig = getSourceConfig(sourceName);
            if (!sourceConfig.isEnabled()) continue;

            try {
                startSource(sourceName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed creating source "+sourceName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public void destroy() throws Exception {

        Collection<String> names = new ArrayList<String>();
        names.addAll(sources.keySet());

        for (String sourceName : names) {
            try {
                stopSource(sourceName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed removing source "+sourceName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public Collection<String> getSourceNames() {
        return sourceConfigManager.getSourceNames();
    }

    public SourceConfig getSourceConfig(String name) {
        return sourceConfigManager.getSourceConfig(name);
    }

    public void startSource(String sourceName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Starting source "+sourceName+".");

        SourceConfig sourceConfig = getSourceConfig(sourceName);
        Source source = createSource(sourceConfig);

        sources.put(sourceName, source);

        String connectionName = source.getConnectionName();
        Collection<Source> list = sourcesByConnectionName.get(connectionName);
        if (list == null) {
            list = new ArrayList<Source>();
            sourcesByConnectionName.put(connectionName, list);
        }
        list.add(source);
    }

    public void stopSource(String sourceName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Stopping source "+sourceName+".");

        Source source = sources.get(sourceName);
        source.destroy();

        sources.remove(sourceName);

        String connectionName = source.getConnectionName();
        Collection<Source> list = sourcesByConnectionName.get(connectionName);
        if (list != null) {
            list.remove(source);
            if (list.isEmpty()) sourcesByConnectionName.remove(connectionName);
        }
    }

    public boolean isRunning(String name) {
        return sources.containsKey(name);
    }

    public Source createSource(SourceConfig sourceConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        String sourceName = sourceConfig.getName();

        if (debug) log.debug("Creating source "+sourceName+".");

        String partitionName = sourceConfig.getPartitionName();
        String connectionName = sourceConfig.getConnectionName();

        Partition sourcePartition;

        if (partitionName == null) {
            sourcePartition = partition;

        } else {
            sourcePartition = partition.getPartitionContext().getPartition(partitionName);
            if (sourcePartition == null) throw new Exception("Unknown partition "+partitionName+".");
        }

        String className = sourceConfig.getSourceClass();

        SourceContext sourceContext = new SourceContext();
        sourceContext.setPartition(partition);

        Source source;

        if (connectionName != null) {
            ConnectionManager connectionManager = sourcePartition.getConnectionManager();
            Connection connection = connectionManager.getConnection(connectionName);
            if (connection == null) throw new Exception("Unknown connection "+connectionName+".");

            Adapter adapter = connection.getAdapter();

            sourceContext.setAdapter(adapter);
            sourceContext.setConnection(connection);

            if (className == null) className = adapter.getSourceClassName();
        }

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        if (debug) log.debug("Creating "+className+".");
        Class clazz = cl.loadClass(className);
        source = (Source)clazz.newInstance();

        source.init(sourceConfig, sourceContext);

        return source;
    }

    public void updateSource(SourceConfig sourceConfig) throws Exception {

        sourceConfigManager.updateSourceConfig(sourceConfig);

        String sourceName = sourceConfig.getName();

        if (isRunning(sourceName)) {
            stopSource(sourceName);
            startSource(sourceName);
        }

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        for (EntryConfig entryConfig : directoryConfig.getEntryConfigs()) {

            for (EntrySourceConfig sourceMapping : entryConfig.getSourceConfigs()) {
                if (!sourceMapping.getSourceName().equals(sourceName)) continue;
                sourceMapping.setSourceName(sourceConfig.getName());
            }
        }
    }

    public Collection<Source> getSources() {
        return sources.values();
    }

    public Source getSource(String name) {
        Source source = sources.get(name);
        if (source != null) return source;

        if (partition.getName().equals(PartitionConfig.ROOT)) return null;
        Partition rootPartition = partition.getPartitionContext().getPartition(PartitionConfig.ROOT);

        SourceManager sourceManager = rootPartition.getSourceManager();
        return sourceManager.getSource(name);
    }

    public Collection<Source> getSourcesByConnectionName(String connectionName) {
        return sourcesByConnectionName.get(connectionName);
    }

    public SourceConfigManager getSourceConfigManager() {
        return sourceConfigManager;
    }
}
