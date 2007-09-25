package org.safehaus.penrose.partition;

/**
 * @author Endi Sukma Dewata
 */
public class DefaultPartitionConfig extends PartitionConfig {

    public DefaultPartitionConfig() {
        super("DEFAULT");
        partitionClass = DefaultPartition.class.getName();
    }
}
