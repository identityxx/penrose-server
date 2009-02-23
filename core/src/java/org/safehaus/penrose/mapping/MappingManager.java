package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class MappingManager {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected Partition partition;
    protected MappingConfigManager mappingConfigManager;

    protected Map<String,Mapping> mappings = new LinkedHashMap<String,Mapping>();

    public MappingManager(Partition partition) {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        mappingConfigManager = partitionConfig.getMappingConfigManager();
    }

    public void init() throws Exception {

        Collection<String> mappingNames = new ArrayList<String>();
        mappingNames.addAll(getMappingNames());

        for (String moduleName : mappingNames) {

            MappingConfig mappingConfig = getMappingConfig(moduleName);
            if (!mappingConfig.isEnabled()) continue;

            startMapping(moduleName);
        }
    }

    public void destroy() throws Exception {

        Collection<String> mappingNames = new ArrayList<String>();
        mappingNames.addAll(mappings.keySet());

        for (String name : mappingNames) {
            stopMapping(name);
        }
    }

    public Collection<String> getMappingNames() {
        return mappingConfigManager.getMappingNames();
    }

    public MappingConfig getMappingConfig(String mappingName) {
        return mappingConfigManager.getMappingConfig(mappingName);
    }

    public void startMapping(String mappingName) throws Exception {

        if (debug) log.debug("Starting mapping "+mappingName+".");

        MappingConfig mappingConfig = getMappingConfig(mappingName);
        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        MappingContext connectionContext = new MappingContext();
        connectionContext.setPartition(partition);
        connectionContext.setClassLoader(cl);

        String mappingClass = mappingConfig.getMappingClass();

        Mapping mapping;
        if (mappingClass == null) {
            mapping = new Mapping();
        } else {
            Class clazz = cl.loadClass(mappingClass);
            mapping = (Mapping)clazz.newInstance();
        }

        mapping.init(mappingConfig, connectionContext);

        mappings.put(mapping.getName(), mapping);
    }

    public void stopMapping(String mappingName) throws Exception {

        if (debug) log.debug("Stopping mapping "+mappingName+".");

        Mapping mapping = mappings.remove(mappingName);
        mapping.destroy();
    }

    public boolean isRunning(String mappingName) {
        return mappings.containsKey(mappingName);
    }

    public Mapping getMapping(String mappingName) {
        if (mappingName == null) return null;

        Mapping mapping = mappings.get(mappingName);
        if (mapping != null) return mapping;

        if (partition.getName().equals(PartitionConfig.ROOT)) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition(PartitionConfig.ROOT);

        MappingManager mappingManager = defaultPartition.getMappingManager();
        return mappingManager.getMapping(mappingName);
    }

    public MappingConfigManager getMappingConfigs() {
        return mappingConfigManager;
    }

    public void updateMappingConfig(String name, MappingConfig mappingConfig) throws Exception {
        mappingConfigManager.updateMappingConfig(name, mappingConfig);
    }

    public MappingConfig removeMappingConfig(String name) {
        return mappingConfigManager.removeMappingConfig(name);
    }

    public Collection<Mapping> getMappings() {
        return mappings.values();
    }
}