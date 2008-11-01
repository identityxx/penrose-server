package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.*;
import org.safehaus.penrose.federation.partition.FederationPartition;
import org.safehaus.penrose.module.Module;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class FederationModule extends Module {

    FederationPartition federationPartition;

    public void init() throws Exception {
        federationPartition = (FederationPartition)partition;
    }

    public void clear() throws Exception {
        federationPartition.clear();
    }

    public Collection<String> getRepositoryNames(String type) throws Exception {
        return federationPartition.getRepositoryNames(type);
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return federationPartition.getRepositoryNames();
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return federationPartition.getRepositories();
    }

    public Collection<FederationRepositoryConfig> getRepositories(String type) {
        return federationPartition.getRepositories(type);
    }

    public FederationRepositoryConfig getRepository(String name) throws Exception {
        return federationPartition.getRepository(name);
    }

    public void addRepository(FederationRepositoryConfig repository) throws Exception {
        federationPartition.addRepository(repository);
    }

    public void removeRepository(String name) throws Exception {
        federationPartition.removeRepository(name);
    }
}