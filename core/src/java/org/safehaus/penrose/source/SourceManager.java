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

        for (SourceConfig sourceConfig : sourceConfigManager.getSourceConfigs()) {
            if (!sourceConfig.isEnabled()) continue;

            createSource(sourceConfig);
        }
    }

    public SourceConfigManager getSourceConfigManager() {
        return sourceConfigManager;
    }

    public Source createSource(
            SourceConfig sourceConfig
    ) throws Exception {

        if (log.isDebugEnabled()) log.debug("Creating source "+sourceConfig.getName()+".");

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
        Class clazz = cl.loadClass(className);

        log.debug("Creating "+className+".");
        source = (Source)clazz.newInstance();
        source.init(sourceConfig, sourceContext);

        addSource(source);

        return source;
    }

    public void addSource(Source source) {

        String name = source.getName();
        sources.put(name, source);

        String connectionName = source.getConnectionName();
        Collection<Source> list = sourcesByConnectionName.get(connectionName);
        if (list == null) {
            list = new ArrayList<Source>();
            sourcesByConnectionName.put(connectionName, list);
        }
        list.add(source);
    }

    public void updateSourceConfig(String name, SourceConfig sourceConfig) throws Exception {

        sourceConfigManager.updateSourceConfig(name, sourceConfig);

        // fix references
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        for (EntryConfig entryConfig : directoryConfig.getEntryConfigs()) {

            for (EntrySourceConfig sourceMapping : entryConfig.getSourceConfigs()) {
                if (!sourceMapping.getSourceName().equals(name)) continue;
                sourceMapping.setSourceName(sourceConfig.getName());
            }
        }
    }

    public Collection<Source> getSources() {
        return sources.values();
    }

    public Source getSource() {
        if (sources.isEmpty()) return null;
        return sources.values().iterator().next();
    }

    public Source getSource(String name) {
        Source source = sources.get(name);
        if (source != null) return source;

        if (partition.getName().equals("DEFAULT")) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition("DEFAULT");

        SourceManager sourceManager = defaultPartition.getSourceManager();
        return sourceManager.getSource(name);
    }

    public Source removeSource(String name) {
        Source source = sources.remove(name);

        String connectionName = source.getConnectionName();
        Collection<Source> list = sourcesByConnectionName.get(connectionName);
        if (list != null) {
            list.remove(source);
            if (list.isEmpty()) sourcesByConnectionName.remove(connectionName);
        }

        return source;
    }

    public Collection<Source> getSourcesByConnectionName(String connectionName) {
        return sourcesByConnectionName.get(connectionName);
    }

    public void destroy() throws Exception {

        for (Source source : sources.values()) {
            source.destroy();
        }
    }
}
