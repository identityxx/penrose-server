package org.safehaus.penrose.partition.event;

import org.safehaus.penrose.partition.Partition;

import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionEvent {

    public final static int PARTITION_STARTED = 0;
    public final static int PARTITION_STOPPED = 1;

    protected Date time;
    protected int action;
    protected Partition partition;

    public PartitionEvent(int action, Partition partition) {
        this.time = new Date();
        this.action = action;
        this.partition = partition;
    }
    
    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
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
}
