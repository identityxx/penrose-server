package org.safehaus.penrose.federation;

import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.module.ModuleManagerClient;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class FederationRepositoryClient {

    protected FederationClient federationClient;
    protected ModuleClient moduleClient;

    public FederationRepositoryClient(FederationClient federationClient, String type) throws Exception {
        this.federationClient = federationClient;

        PartitionClient partitionClient = federationClient.getPartitionClient();
        ModuleManagerClient moduleManagerClient = partitionClient.getModuleManagerClient();
        moduleClient = moduleManagerClient.getModuleClient(type);
    }

    public FederationClient getFederationClient() {
        return federationClient;
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return (Collection<String>)moduleClient.getAttribute("RepositoryNames");
    }

    public FederationRepositoryConfig getRepository(String name) throws Exception {
        return (FederationRepositoryConfig)moduleClient.invoke(
                "getRepository",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return (Collection<FederationRepositoryConfig>)moduleClient.getAttribute("Repositories");
    }

    public void addRepository(FederationRepositoryConfig repository) throws Exception {
        federationClient.addRepository(repository);
        federationClient.storeFederationConfig();
    }

    public void updateRepository(FederationRepositoryConfig repository) throws Exception {

        String name = repository.getName();

        federationClient.stopPartition(name);
        federationClient.removePartition(name);

        federationClient.removeRepository(name);
        federationClient.addRepository(repository);
        federationClient.storeFederationConfig();

        federationClient.createPartition(name);
        federationClient.startPartition(name);
    }

    public void removeRepository(String name) throws Exception {
        federationClient.removeRepository(name);
        federationClient.storeFederationConfig();
    }

    public void createPartitions(String name) throws Exception {
        federationClient.createPartition(name);
        federationClient.startPartition(name);
    }

    public void removePartitions(String name) throws Exception {
        federationClient.stopPartition(name);
        federationClient.removePartition(name);
    }
}
