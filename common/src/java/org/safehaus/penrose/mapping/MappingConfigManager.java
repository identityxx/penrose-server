package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class MappingConfigManager implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(MappingConfigManager.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    private Map<String,MappingConfig> mappingConfigs = new LinkedHashMap<String,MappingConfig>();

    public void addMappingConfig(MappingConfig mappingConfig) {
        mappingConfigs.put(mappingConfig.getName(), mappingConfig);
    }

    public MappingConfig getMappingConfig(String name) {
        return mappingConfigs.get(name);
    }

    public Collection<String> getMappingNames() {
        return mappingConfigs.keySet();
    }

    public Collection<MappingConfig> getMappingConfigs() {
        return mappingConfigs.values();
    }

    public void updateMappingConfig(String name, MappingConfig mappingConfig) throws Exception {

        MappingConfig oldMappingConfig = mappingConfigs.get(name);
        oldMappingConfig.copy(mappingConfig);

        if (!name.equals(mappingConfig.getName())) {
            mappingConfigs.remove(name);
            mappingConfigs.put(mappingConfig.getName(), mappingConfig);
        }
    }

    public MappingConfig removeMappingConfig(String name) {
        return mappingConfigs.remove(name);
    }

    public void copy(MappingConfigManager mappingConfigManager) throws CloneNotSupportedException {
        mappingConfigs = new LinkedHashMap<String,MappingConfig>();
        for (MappingConfig mappingConfig : mappingConfigManager.mappingConfigs.values()) {
            mappingConfigs.put(mappingConfig.getName(), (MappingConfig)mappingConfig.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {
        MappingConfigManager mappingConfigManager = (MappingConfigManager)super.clone();
        mappingConfigManager.copy(this);
        return mappingConfigManager;
    }
}