package org.safehaus.penrose.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.SourceSync;
import org.safehaus.penrose.directory.SourceMapping;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.ldap.DN;

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

    public void addPartition(Partition partition) {
        partitions.put(partition.getName(), partition);
    }

    public Partition removePartition(String name) {
        return partitions.remove(name);
    }

    public void stop() throws Exception {
        for (Partition partition : partitions.values()) {

            for (Module module : partition.getModules()) {
                module.destroy();
            }

            for (SourceSync sourceSync : partition.getSourceSyncs()) {
                sourceSync.destroy();
            }

            for (Connection connection : partition.getConnections().getConnections()) {
                connection.destroy();
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

    public Partition getPartition(DN dn) throws Exception {

        if (debug) log.debug("Finding partition for \""+dn+"\".");

        if (dn == null) {
            log.debug("DN is null.");
            return getPartition("DEFAULT");
        }

        Partition p = getPartition("DEFAULT");
        DN s = null;

        for (Partition partition : partitions.values()) {
            if (debug) log.debug("Checking "+partition.getName()+" partition.");

            PartitionConfig partitionConfig = partition.getPartitionConfig();
            Collection<DN> suffixes = partitionConfig.getDirectoryConfig().getSuffixes();
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

    public int size() {
        return partitions.size();
    }
}
