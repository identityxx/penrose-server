package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.Penrose;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class MappingManager {

    public Logger log = LoggerFactory.getLogger(getClass());

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

        for (String mappingName : mappingNames) {

            MappingConfig mappingConfig = getMappingConfig(mappingName);
            if (!mappingConfig.isEnabled()) continue;

            try {
                startMapping(mappingName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed creating mapping "+mappingName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public void destroy() throws Exception {

        Collection<String> mappingNames = new ArrayList<String>();
        mappingNames.addAll(mappings.keySet());

        for (String mappingName : mappingNames) {
            try {
                stopMapping(mappingName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed removing mapping "+mappingName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public Collection<String> getMappingNames() {
        return mappingConfigManager.getMappingNames();
    }

    public MappingConfig getMappingConfig(String mappingName) {
        return mappingConfigManager.getMappingConfig(mappingName);
    }

    public void startMapping(String mappingName) throws Exception {

        boolean debug = log.isDebugEnabled();
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

        boolean debug = log.isDebugEnabled();
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
        Partition rootPartition = partition.getPartitionContext().getPartition(PartitionConfig.ROOT);

        MappingManager mappingManager = rootPartition.getMappingManager();
        return mappingManager.getMapping(mappingName);
    }

    public MappingConfigManager getMappingConfigs() {
        return mappingConfigManager;
    }

    public void updateMappingConfig(String mappingName, MappingConfig mappingConfig) throws Exception {
        mappingConfigManager.updateMappingConfig(mappingName, mappingConfig);
    }

    public MappingConfig removeMappingConfig(String name) {
        return mappingConfigManager.removeMappingConfig(name);
    }

    public Collection<Mapping> getMappings() {
        return mappings.values();
    }
}