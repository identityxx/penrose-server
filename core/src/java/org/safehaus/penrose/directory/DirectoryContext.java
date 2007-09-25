package org.safehaus.penrose.directory;

import org.safehaus.penrose.partition.Partition;

/**
 * @author Endi Sukma Dewata
 */
public class DirectoryContext {
    
    private Partition partition;

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
