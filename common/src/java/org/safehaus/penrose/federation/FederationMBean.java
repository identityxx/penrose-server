package org.safehaus.penrose.federation;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface FederationMBean {

    public Collection<String> getRepositoryNames() throws Exception;
    public Collection<Repository> getRepositories(String type) throws Exception;
    public Repository getRepository(String name) throws Exception;

    public void createPartitions() throws Exception;
    public void createPartitions(String name) throws Exception;
    
    public void startPartitions() throws Exception;
    public void startPartitions(String name) throws Exception;

    public void stopPartitions() throws Exception;
    public void stopPartitions(String name) throws Exception;

    public void removePartitions() throws Exception;
    public void removePartitions(String name) throws Exception;
    
    public void synchronize(String name) throws Exception;
}
