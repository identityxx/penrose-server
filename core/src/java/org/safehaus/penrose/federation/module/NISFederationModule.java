package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.federation.Federation;
import org.safehaus.penrose.federation.SynchronizationResult;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.nis.module.NISSynchronizationModule;
import org.safehaus.penrose.module.Module;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class NISFederationModule extends Module {

    public final static String NIS        = "nis";

    public SynchronizationResult synchronizeNISMaps(String name, Collection<String> maps) throws Exception {

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
