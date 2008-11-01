package org.safehaus.penrose.management.mapping;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.Partition;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class MappingManagerService extends BaseService implements MappingManagerServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    public MappingManagerService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return MappingManagerClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getMappingManager();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public MappingConfigManager getMappingConfigManager() {
        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return null;
        return partitionConfig.getMappingConfigManager();
    }

    public MappingManager getMappingManager() {
        Partition partition = getPartition();
        if (partition == null) return null;
        return partition.getMappingManager();
    }

    public MappingService getMappingService(String mappingName) throws Exception {

        MappingService mappingService = new MappingService(jmxService, partitionManager, partitionName, mappingName);
        mappingService.init();

        return mappingService;
    }

    public void register() throws Exception {

        super.register();

        MappingConfigManager mappingConfigManager = getMappingConfigManager();
        for (String mappingName : mappingConfigManager.getMappingNames()) {
            MappingService mappingService = getMappingService(mappingName);
            mappingService.register();
        }
    }

    public void unregister() throws Exception {
        MappingConfigManager mappingConfigManager = getMappingConfigManager();
        for (String mappingName : mappingConfigManager.getMappingNames()) {
            MappingService mappingService = getMappingService(mappingName);
            mappingService.unregister();
        }

        super.unregister();
    }

    public Collection<String> getMappingNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(mappingConfigManager.getMappingNames());

        return list;
    }

    public void startMapping(String mappingName) throws Exception {

        Partition partition = getPartition();
        if (partition == null) return;

        MappingManager mappingManager = partition.getMappingManager();
        mappingManager.startMapping(mappingName);
    }

    public void stopMapping(String mappingName) throws Exception {

        Partition partition = getPartition();
        if (partition == null) return;

        MappingManager mappingManager = partition.getMappingManager();
        boolean running = mappingManager.isRunning(mappingName);
        if (running) mappingManager.stopMapping(mappingName);
    }

    public void createMapping(MappingConfig mappingConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        mappingConfigManager.addMappingConfig(mappingConfig);

        String mappingName = mappingConfig.getName();
        startMapping(mappingName);

        MappingService mappingService = getMappingService(mappingName);
        mappingService.register();
    }

    public void updateMapping(String mappingName, MappingConfig mappingConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();

        stopMapping(mappingName);

        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        mappingConfigManager.updateMappingConfig(mappingName, mappingConfig);

        startMapping(mappingName);
    }

    public void removeMapping(String mappingName) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();

        MappingService mappingService = getMappingService(mappingName);
        mappingService.unregister();

        stopMapping(mappingName);

        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        mappingConfigManager.removeMappingConfig(mappingName);
    }

}
