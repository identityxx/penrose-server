package org.safehaus.penrose.federation;

import org.safehaus.penrose.module.ModuleClient;

/**
 * @author Endi Sukma Dewata
 */
public class RepositoryClient implements FederationRepositoryMBean {

    protected FederationClient federationClient;
    protected String repositoryName;
    protected ModuleClient moduleClient;

    public RepositoryClient(FederationClient federationClient, String repositoryName, String type) throws Exception {
        this.federationClient = federationClient;
        this.repositoryName = repositoryName;
        this.moduleClient = federationClient.getRepositoryModuleClient(type);
    }

    public FederationClient getFederationClient() {
        return federationClient;
    }

    public String getRepositoryName() {
        return repositoryName;
    }

    public void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    public void setFederationClient(FederationClient federationClient) {
        this.federationClient = federationClient;
    }

    public ModuleClient getModuleClient() {
        return moduleClient;
    }

    public void setModuleClient(ModuleClient moduleClient) {
        this.moduleClient = moduleClient;
    }
}
