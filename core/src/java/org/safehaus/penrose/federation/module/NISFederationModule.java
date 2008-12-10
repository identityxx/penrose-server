package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.Federation;
import org.safehaus.penrose.federation.SynchronizationResult;
import org.safehaus.penrose.federation.FederationRepositoryConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.nis.module.NISSynchronizationModule;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class NISFederationModule extends FederationRepositoryModule {

    public Collection<String> getRepositoryNames() throws Exception {
        return getRepositoryNames("NIS");
    }

    public Collection<FederationRepositoryConfig> getRepositories() throws Exception {
        return getRepositories("NIS");
    }

    public SynchronizationResult synchronize(String name, Collection<String> maps) throws Exception {

        Partition nisPartition = moduleContext.getPartition(name);

        NISSynchronizationModule module = (NISSynchronizationModule)nisPartition.getModuleManager().getModule(Federation.SYNCHRONIZATION);

        if (maps == null || maps.isEmpty()) {
            return module.synchronize();
        }

        SynchronizationResult totalResult = new SynchronizationResult();

        for (String map : maps) {
            SynchronizationResult r = module.synchronizeNISMap(map);
            totalResult.add(r);
        }

        return totalResult;
    }
}
