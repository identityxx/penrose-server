package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleManager;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class FederationRepositoryModule extends Module {

    FederationModule federationModule;

    public void init() throws Exception {
        ModuleManager moduleManager = partition.getModuleManager();
        federationModule = (FederationModule)moduleManager.getModule(Federation.FEDERATION);
    }

    public void clear() throws Exception {
        federationModule.clear();
    }

    public Collection<String> getRepositoryNames(String type) throws Exception {
        return federationModule.getRepositoryNames(type);
    }

    public Collection<String> getRepositoryNames() throws Exception {
        return federationModule.getRepositoryNames();
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return federationModule.getRepositories();
    }

    public Collection<FederationRepositoryConfig> getRepositories(String type) throws Exception {
        return federationModule.getRepositories(type);
    }

    public FederationRepositoryConfig getRepository(String name) throws Exception {
        return federationModule.getRepository(name);
    }

    public void addRepository(FederationRepositoryConfig repository) throws Exception {
        federationModule.addRepository(repository);
    }

    public void removeRepository(String name) throws Exception {
        federationModule.removeRepository(name);
    }
}