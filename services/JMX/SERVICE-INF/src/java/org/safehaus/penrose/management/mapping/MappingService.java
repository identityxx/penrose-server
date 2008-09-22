package org.safehaus.penrose.management.mapping;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.mapping.Mapping;
import org.safehaus.penrose.mapping.MappingConfig;
import org.safehaus.penrose.mapping.MappingManager;
import org.safehaus.penrose.mapping.MappingClient;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;

/**
 * @author Endi Sukma Dewata
 */
public class MappingService extends BaseService implements MappingServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;
    private String mappingName;

    public MappingService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName,
            String mappingName
    ) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
        this.mappingName = mappingName;
    }

    public String getObjectName() {
        return MappingClient.getStringObjectName(partitionName, mappingName);
    }

    public Object getObject() {
        return getMapping();
    }

    public MappingConfig getMappingConfig() throws Exception {
        return getPartitionConfig().getMappingConfigManager().getMappingConfig(mappingName);
    }

    public Mapping getMapping() {
        Partition partition = getPartition();
        if (partition == null) return null;
        MappingManager mappingManager = partition.getMappingManager();
        return mappingManager.getMapping(mappingName);
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public void start() throws Exception {

        log.debug("Starting mapping "+partitionName+"/"+ mappingName +"...");

        Partition partition = getPartition();
        MappingManager mappingManager = partition.getMappingManager();
        mappingManager.startMapping(mappingName);

        log.debug("Mapping started.");
    }

    public void stop() throws Exception {

        log.debug("Stopping mapping "+partitionName+"/"+ mappingName +"...");

        Partition partition = getPartition();
        MappingManager mappingManager = partition.getMappingManager();
        mappingManager.stopMapping(mappingName);

        log.debug("Mapping stopped.");
    }

    public void restart() throws Exception {

        log.debug("Restarting mapping "+partitionName+"/"+ mappingName +"...");

        Partition partition = getPartition();
        MappingManager mappingManager = partition.getMappingManager();
        mappingManager.stopMapping(mappingName);
        mappingManager.startMapping(mappingName);

        log.debug("Mapping restarted.");
    }
}