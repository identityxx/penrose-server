package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

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

        for (MappingConfig mappingConfig : mappingConfigManager.getMappingConfigs()) {
            if (!mappingConfig.isEnabled()) continue;

            createMapping(mappingConfig);
        }
    }

    public void destroy() throws Exception {
        for (Mapping mapping : mappings.values()) {
            if (debug) log.debug("Stopping "+ mapping.getName()+" mapping.");
            mapping.destroy();
        }
    }

    public MappingConfig getMappingConfig(String name) {
        return mappingConfigManager.getMappingConfig(name);
    }

    public void startMapping(String name) throws Exception {
        MappingConfig connectionConfig = mappingConfigManager.getMappingConfig(name);
        createMapping(connectionConfig);
    }

    public void stopMapping(String name) throws Exception {
        Mapping connection = mappings.remove(name);
        connection.destroy();
    }

    public boolean isRunning(String name) {
        return mappings.containsKey(name);
    }

    public Mapping createMapping(MappingConfig mappingConfig) throws Exception {

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

        addMapping(mapping);

        return mapping;
    }

    public void addMapping(Mapping mapping) {
        mappings.put(mapping.getName(), mapping);
    }

    public void removeConnection(String name) throws Exception {
        Mapping mapping = mappings.remove(name);
        if (mapping != null) mapping.destroy();
        mappingConfigManager.removeMappingConfig(name);
    }

    public Mapping getMapping(String name) {
        Mapping mapping = mappings.get(name);
        if (mapping != null) return mapping;

        if (partition.getName().equals("DEFAULT")) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition("DEFAULT");

        MappingManager mappingManager = defaultPartition.getMappingManager();
        return mappingManager.getMapping(name);
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