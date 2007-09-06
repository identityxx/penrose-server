package org.safehaus.penrose.scheduler;

import org.safehaus.penrose.partition.Partition;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerContext {
    
    private Partition partition;

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
