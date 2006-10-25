package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.PartitionManagerMBean;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class PartitionManagerService implements PartitionManagerMBean {

    private PartitionManager partitionManager;

    public PartitionManagerService(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;
    }

    public Collection getPartitionNames() throws Exception {
        return partitionManager.getPartitionNames();
    }

    public PartitionConfig getPartitionConfig(String name) throws Exception {
        return partitionManager.getPartitionConfig(name);
    }

    public String getStatus(String name) throws Exception {
        return partitionManager.getStatus(name);
    }

    public void start(String name) throws Exception {
        partitionManager.start(name);
    }

    public void stop(String name) throws Exception {
        partitionManager.stop(name);
    }

    public void restart(String name) throws Exception {
        partitionManager.restart(name);
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }
}
