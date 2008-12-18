package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.Federation;
import org.safehaus.penrose.federation.SynchronizationResult;
import org.safehaus.penrose.federation.FederationRepositoryConfig;
import org.safehaus.penrose.federation.NISRepositoryMBean;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.nis.module.NISSynchronizationModule;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class NISRepositoryModule extends FederationRepositoryModule implements NISRepositoryMBean {

    public Collection<String> getRepositoryNames() throws Exception {
        return getRepositoryNames("NIS");
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return getRepositories("NIS");
    }

    public SynchronizationResult synchronize(String repositoryName, Collection<String> mapNames) throws Exception {

        String federationDomain = partition.getName();
        Partition nisPartition = moduleContext.getPartition(federationDomain+"_"+repositoryName);
        
        if (nisPartition == null) {
            throw new Exception("Unknown NIS repository: "+repositoryName);
        }

        NISSynchronizationModule module = (NISSynchronizationModule)nisPartition.getModuleManager().getModule(Federation.SYNCHRONIZATION);

        if (mapNames == null || mapNames.isEmpty()) {
            return module.synchronize();
        }

        SynchronizationResult totalResult = new SynchronizationResult();

        for (String map : mapNames) {
            SynchronizationResult r = module.synchronizeNISMap(map);
            totalResult.add(r);
        }

        return totalResult;
    }
}
