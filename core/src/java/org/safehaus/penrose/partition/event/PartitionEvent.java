package org.safehaus.penrose.partition.event;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;

import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionEvent {

    public final static int PARTITION_ADDED   = 0;
    public final static int PARTITION_REMOVED = 1;
    public final static int PARTITION_STARTED = 2;
    public final static int PARTITION_STOPPED = 3;

    protected Date time;
    protected int action;

    private String partitionName;

    protected PartitionConfig partitionConfig;
    protected Partition partition;

    public PartitionEvent(int action, PartitionConfig partitionConfig) {
        this.time = new Date();
        this.action = action;
        this.partitionName = partitionConfig.getName();
        this.partitionConfig = partitionConfig;
    }

    public PartitionEvent(int action, Partition partition) {
        this.time = new Date();
        this.action = action;
        this.partitionName = partition.getName();
        this.partitionConfig = partition.getPartitionConfig();
        this.partition = partition;
    }
    
    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public PartitionConfig getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }
}
