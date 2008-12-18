package org.safehaus.penrose.federation;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface FederationMBean {

    public FederationConfig getFederationConfig() throws Exception;
    public void setFederationConfig(FederationConfig federationConfig) throws Exception;

    public void load() throws Exception;
    public void store() throws Exception;
    public void clear() throws Exception;

    public Collection<String> getRepositoryTypes() throws Exception;

    public Collection<String> getRepositoryNames() throws Exception;
    public Collection<String> getRepositoryNames(String type) throws Exception;

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception;
    public Collection<FederationRepositoryConfig> getRepositories(String type) throws Exception;
    public FederationRepositoryConfig getRepository(String name) throws Exception;

    public void addRepository(FederationRepositoryConfig repository) throws Exception;
    public void removeRepository(String name) throws Exception;
    public void updateRepository(FederationRepositoryConfig repository) throws Exception;

    public Collection<String> getPartitionNames() throws Exception;
    public Collection<FederationPartitionConfig> getPartitions() throws Exception;
    public FederationPartitionConfig getPartition(String partitionName) throws Exception;

    public void createPartition(String partitionName) throws Exception;
    public void removePartition(String partitionName) throws Exception;
    
    public void synchronize(String name) throws Exception;
}
