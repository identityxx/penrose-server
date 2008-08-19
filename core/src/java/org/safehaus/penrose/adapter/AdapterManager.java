package org.safehaus.penrose.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class AdapterManager {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Partition partition;

    protected Map<String,Adapter> adapters = new LinkedHashMap<String,Adapter>();

    public void init(Partition partition) throws Exception {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        for (AdapterConfig adapterConfig : partitionConfig.getAdapterConfigs()) {
            Adapter adapter = createAdapter(adapterConfig);
            addAdapter(adapter);
        }
    }

    public void destroy() throws Exception {
        for (Adapter adapter : adapters.values()) {
            adapter.destroy();
        }
    }

    public Adapter createAdapter(AdapterConfig adapterConfig) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();

        String adapterClass = adapterConfig.getAdapterClass();
        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(adapterClass);
        Adapter adapter = (Adapter)clazz.newInstance();

        AdapterContext adapterContext = new AdapterContext();
        adapterContext.setPartition(partition);

        adapter.init(adapterConfig, adapterContext);

        return adapter;
    }

    public void addAdapter(Adapter adapter) {
        adapters.put(adapter.getName(), adapter);
    }

    public Collection<Adapter> getAdapters() {
        return adapters.values();
    }

    public Adapter getAdapter(String name) {
        Adapter adapter = adapters.get(name);
        if (adapter != null) return adapter;

        if (partition.getName().equals("DEFAULT")) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition("DEFAULT");

        AdapterManager adapterManager = defaultPartition.getAdapterManager();
        return adapterManager.getAdapter(name);
    }
}
