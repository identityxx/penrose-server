package org.safehaus.penrose.federation;

import org.apache.log4j.Logger;
import org.safehaus.penrose.module.ModuleClient;
import org.safehaus.penrose.partition.PartitionClient;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class LDAPFederationClient {

    public final static String LOCAL_CONNECTION = "LDAP";
    public final static String LOCAL_SOURCE = "Local";

    public final static String GLOBAL_CONNECTION = "Global";
    public final static String GLOBAL_SOURCE = "Global";

    public Logger log = Logger.getLogger(getClass());

    private FederationClient federationClient;
    private ModuleClient moduleClient;

    public LDAPFederationClient(FederationClient federationClient) throws Exception {
        this.federationClient = federationClient;
        PartitionClient partitionClient = federationClient.getPartitionClient();
        moduleClient = partitionClient.getModuleClient("LDAP");
    }

    public FederationClient getFederationClient() {
        return federationClient;
    }
    
    public FederationRepositoryConfig getRepository(String name) throws Exception {
        return federationClient.getRepository(name);
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        Collection<FederationRepositoryConfig> list = new ArrayList<FederationRepositoryConfig>();
        for (FederationRepositoryConfig repository : federationClient.getRepositories("LDAP")) {
            list.add(repository);
        }
        return list;
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
