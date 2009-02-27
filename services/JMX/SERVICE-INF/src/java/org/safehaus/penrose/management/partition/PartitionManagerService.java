package org.safehaus.penrose.management.partition;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionManagerClient;
import org.safehaus.penrose.partition.PartitionManagerServiceMBean;
import org.safehaus.penrose.partition.event.PartitionEvent;
import org.safehaus.penrose.partition.event.PartitionListener;
import org.safehaus.penrose.util.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionManagerService extends BaseService implements PartitionManagerServiceMBean, PartitionListener {

    PartitionManager partitionManager;

    public PartitionManagerService(PenroseJMXService jmxService, PartitionManager partitionManager) {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;

        partitionManager.addListener(this);
    }

    public Object getObject() {
        return partitionManager;
    }

    public String getObjectName() {
        return PartitionManagerClient.getStringObjectName();
    }

    public Collection<String> getPartitionNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(partitionManager.getAvailablePartitionNames());
        return list;
    }
    
    public void storePartition(String name) throws Exception {
        partitionManager.storePartition(name);
    }

    public void loadPartition(String name) throws Exception {
        partitionManager.loadPartition(name);
    }

    public void unloadPartition(String name) throws Exception {
        partitionManager.removePartition(name);
    }

    public void startPartition(String name) throws Exception {
        partitionManager.startPartition(name);
    }

    public void stopPartition(String name) throws Exception {
        partitionManager.stopPartition(name);
    }

    public void startPartitions() throws Exception {
        partitionManager.startPartitions();
    }

    public void stopPartitions() throws Exception {
        partitionManager.stopPartitions();
    }

    public PartitionConfig getPartitionConfig(String partitionName) throws Exception {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public void addPartition(PartitionConfig partitionConfig) throws Exception {

        partitionManager.addPartitionConfig(partitionConfig);
        partitionManager.storePartition(partitionConfig.getName());
    }

    public void updatePartition(String partitionName, PartitionConfig partitionConfig) throws Exception {

        partitionManager.stopPartition(partitionName);
        partitionManager.removePartition(partitionName);

        File partitionsDir = partitionManager.getPartitionsDir();
        File oldDir = new File(partitionsDir, partitionName);
        File newDir = new File(partitionsDir, partitionConfig.getName());
        oldDir.renameTo(newDir);

        partitionManager.addPartitionConfig(partitionConfig);
        partitionManager.storePartition(partitionConfig.getName());
        partitionManager.startPartition(partitionConfig.getName());
    }

    public void removePartition(String partitionName) throws Exception {

        PartitionService partitionService = getPartitionService(partitionName);
        partitionService.unregister();

        File partitionsDir = partitionManager.getPartitionsDir();
        File partitionDir = new File(partitionsDir, partitionName);

        partitionManager.stopPartition(partitionName);
        partitionManager.removePartition(partitionName);

        FileUtil.delete(partitionDir);
    }

    public PartitionService getPartitionService(String partitionName) throws Exception {

        PartitionService service = new PartitionService(jmxService, partitionManager, partitionName);
        service.init();

        return service;
    }

    public void register() throws Exception {
        super.register();

        PartitionService rootPartition = getPartitionService(PartitionConfig.ROOT);
        rootPartition.register();

        for (PartitionConfig partitionConfig : partitionManager.getPartitionConfigs()) {
            String partitionName = partitionConfig.getName();
            PartitionService partitionService = getPartitionService(partitionName);
            partitionService.register();
        }
    }

    public void unregister() throws Exception {

        for (PartitionConfig partitionConfig : partitionManager.getPartitionConfigs()) {
            String partitionName = partitionConfig.getName();
            PartitionService partitionService = getPartitionService(partitionName);
            partitionService.unregister();
        }

        PartitionService rootPartition = getPartitionService(PartitionConfig.ROOT);
        rootPartition.unregister();

        super.unregister();
    }

    public void partitionAdded(PartitionEvent event) throws Exception {

        String partitionName = event.getPartitionName();

        PartitionService partitionService = getPartitionService(partitionName);
        partitionService.register();
    }

    public void partitionRemoved(PartitionEvent event) throws Exception {

        String partitionName = event.getPartitionName();

        PartitionService partitionService = getPartitionService(partitionName);
        partitionService.unregister();
    }

    public void partitionStarted(PartitionEvent event) throws Exception {
    }

    public void partitionStopped(PartitionEvent event) throws Exception {
    }
}
