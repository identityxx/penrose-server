package org.safehaus.penrose.directory;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.session.SessionManager;

/**
 * @author Endi Sukma Dewata
 */
public class EntryContext {

    private Partition partition;
    private Directory directory;

    public EntryContext() {
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
