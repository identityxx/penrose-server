package org.safehaus.penrose.management.partition;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionWriter;
import org.safehaus.penrose.util.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionManagerService extends BaseService implements PartitionManagerServiceMBean {

    PartitionManager partitionManager;

    public PartitionManagerService(PenroseJMXService jmxService, PartitionManager partitionManager) {
        super(PartitionManagerServiceMBean.class);

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
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
    
    public void startPartitions() throws Exception {
        partitionManager.startPartitions();
    }

    public void stopPartitions() throws Exception {
        partitionManager.stopPartitions();
    }

    public PartitionConfig getPartitionConfig(String partitionName) throws Exception {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public void createPartition(PartitionConfig partitionConfig) throws Exception {

        String partitionName = partitionConfig.getName();

        File partitionsDir = partitionManager.getPartitionsDir();
        File path = new File(partitionsDir, partitionConfig.getName());
        partitionConfig.store(path);

        partitionManager.addPartitionConfig(partitionConfig);
        partitionManager.startPartition(partitionName);

        PartitionService partitionService = getPartitionService(partitionName);
        partitionService.register();
    }

    public void updatePartition(String name, PartitionConfig partitionConfig) throws Exception {

        PartitionService oldService = getPartitionService(name);
        oldService.unregister();

        partitionManager.stopPartition(name);
        partitionManager.unloadPartition(name);

        File partitionsDir = partitionManager.getPartitionsDir();
        File oldDir = new File(partitionsDir, name);
        File newDir = new File(partitionsDir, partitionConfig.getName());
        oldDir.renameTo(newDir);

        partitionConfig.store(newDir);

        partitionManager.addPartitionConfig(partitionConfig);
        partitionManager.startPartition(partitionConfig.getName());

        PartitionService newService = getPartitionService(partitionConfig.getName());
        newService.register();
    }

    public void removePartition(String name) throws Exception {

        File partitionsDir = partitionManager.getPartitionsDir();
        File partitionDir = new File(partitionsDir, name);

        PartitionService partitionService = getPartitionService(name);
        partitionService.unregister();

        partitionManager.stopPartition(name);
        partitionManager.unloadPartition(name);

        FileUtil.delete(partitionDir);
    }

    public PartitionService getPartitionService(String partitionName) throws Exception {

        PartitionService service = new PartitionService(jmxService, partitionManager, partitionName);
        service.init();

        return service;
    }

    public void register() throws Exception {
        super.register();

        PartitionService defaultPartitionService = getPartitionService("DEFAULT");
        defaultPartitionService.register();

        for (String partitionName : partitionManager.getAvailablePartitionNames()) {
            PartitionService partitionService = getPartitionService(partitionName);
            partitionService.register();
        }
    }

    public void unregister() throws Exception {
        for (String partitionName : partitionManager.getAvailablePartitionNames()) {
            PartitionService partitionService = getPartitionService(partitionName);
            partitionService.unregister();
        }

        PartitionService defaultPartitionService = getPartitionService("DEFAULT");
        defaultPartitionService.unregister();

        super.unregister();
    }
}
