package org.safehaus.penrose.partition.event;

/**
 * @author Endi Sukma Dewata
 */
public interface PartitionListener {

    public void partitionAdded(PartitionEvent event) throws Exception;
    public void partitionRemoved(PartitionEvent event) throws Exception;

    public void partitionStarted(PartitionEvent event) throws Exception;
    public void partitionStopped(PartitionEvent event) throws Exception;
}
