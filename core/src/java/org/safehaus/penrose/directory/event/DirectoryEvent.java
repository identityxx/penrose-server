package org.safehaus.penrose.directory.event;

import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryEvent {

    public final static int ENTRY_ADDED   = 0;
    public final static int ENTRY_REMOVED = 1;
    
    protected Date time;
    protected int action;

    protected String partitionName;
    protected String entryName;

    public DirectoryEvent(int action, String partitionName, String entryName) {
        this.time = new Date();
        this.action = action;
        this.partitionName = partitionName;
        this.entryName = entryName;
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

    public String getEntryName() {
        return entryName;
    }

    public void setEntryName(String entryName) {
        this.entryName = entryName;
    }
}
