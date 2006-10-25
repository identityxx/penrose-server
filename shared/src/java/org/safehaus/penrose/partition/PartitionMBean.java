package org.safehaus.penrose.partition;

/**
 * @author Endi S. Dewata
 */
public interface PartitionMBean {

    public String getName() throws Exception;
    public PartitionConfig getPartitionConfig() throws Exception;
    public String getStatus() throws Exception;

    public void start() throws Exception;
    public void stop() throws Exception;
    public void restart() throws Exception;
}
