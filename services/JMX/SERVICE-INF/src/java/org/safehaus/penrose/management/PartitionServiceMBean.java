package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.PartitionConfig;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface PartitionServiceMBean {

    public String getStatus() throws Exception;

    public PartitionConfig getPartitionConfig() throws Exception;

    public Collection<String> getConnectionNames() throws Exception;
    public Collection<String> getSourceNames() throws Exception;
    public Collection<String> getModuleNames() throws Exception;
}
