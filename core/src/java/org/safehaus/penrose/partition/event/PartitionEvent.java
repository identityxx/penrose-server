package org.safehaus.penrose.partition.event;

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

    public PartitionEvent(int action, String partitionName) {
        this.time = new Date();
        this.action = action;
        this.partitionName = partitionName;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
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
