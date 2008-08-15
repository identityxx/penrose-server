package org.safehaus.penrose.federation;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface FederationMBean {

    public Collection<String> getRepositoryNames() throws Exception;
    public Repository getRepository(String name) throws Exception;

    public void createPartitions(String name) throws Exception;
    public void removePartitions(String name) throws Exception;
    
    public void synchronize(String name) throws Exception;
    public void synchronize(String name, Collection<String> parameters) throws Exception;
}
