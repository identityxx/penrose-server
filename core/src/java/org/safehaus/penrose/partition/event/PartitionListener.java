package org.safehaus.penrose.partition.event;

/**
 * @author Endi Sukma Dewata
 */
public interface PartitionListener {

    public void partitionStarted(PartitionEvent event) throws Exception;
    public void partitionStopped(PartitionEvent event) throws Exception;
}
