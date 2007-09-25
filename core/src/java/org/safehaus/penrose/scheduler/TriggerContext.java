package org.safehaus.penrose.scheduler;

import org.safehaus.penrose.partition.Partition;

/**
 * @author Endi Sukma Dewata
 */
public class TriggerContext {

    private Partition partition;

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
