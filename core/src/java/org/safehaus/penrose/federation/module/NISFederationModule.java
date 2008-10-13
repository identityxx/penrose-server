package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.FederationRepositoryConfig;
import org.safehaus.penrose.federation.Federation;
import org.safehaus.penrose.federation.SynchronizationResult;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.nis.module.NISSynchronizationModule;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class NISFederationModule extends FederationModule {

    public SynchronizationResult synchronizeNISMaps(String name, Collection<String> maps) throws Exception {

        FederationRepositoryConfig repository = federationConfig.getRepository(name);
        if (repository == null) return null;

        Partition nisPartition = getPartition(name+"_"+NIS);

        NISSynchronizationModule module = (NISSynchronizationModule)nisPartition.getModuleManager().getModule(Federation.SYNCHRONIZATION_MODULE);

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
