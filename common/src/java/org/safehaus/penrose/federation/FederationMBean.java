package org.safehaus.penrose.federation;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface FederationMBean {

    public Collection<String> getRepositoryNames() throws Exception;
    public Collection<FederationRepositoryConfig> getRepositories() throws Exception;
    public Collection<FederationRepositoryConfig> getRepositories(String type) throws Exception;
    public FederationRepositoryConfig getRepository(String name) throws Exception;

    public Collection<String> getPartitionNames() throws Exception;
    public Collection<FederationPartitionConfig> getPartitions() throws Exception;
    public FederationPartitionConfig getPartition(String name) throws Exception;

    public void createPartitions() throws Exception;
    public void createPartition(String name) throws Exception;
    
    public void startPartitions() throws Exception;
    public void startPartition(String name) throws Exception;

    public void stopPartitions() throws Exception;
    public void stopPartition(String name) throws Exception;

    public void removePartitions() throws Exception;
    public void removePartition(String name) throws Exception;
    
    public void synchronize(String name) throws Exception;
}
