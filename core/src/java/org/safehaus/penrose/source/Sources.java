package org.safehaus.penrose.source;

import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.SourceMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Sources {

    public Logger log = LoggerFactory.getLogger(getClass());

    private Map<String,SourceConfig> sourceConfigs = new LinkedHashMap<String,SourceConfig>();
    private Map<String,SourceSyncConfig> sourceSyncConfigs = new LinkedHashMap<String,SourceSyncConfig>();

    public void addSourceConfig(SourceConfig sourceConfig) {

        log.debug("Adding source "+sourceConfig.getName());
        
        sourceConfigs.put(sourceConfig.getName(), sourceConfig);

        String sync = sourceConfig.getParameter("sync");
        if (sync != null) {

            log.debug("Sync source with "+sync);

            SourceSyncConfig sourceSyncConfig = new SourceSyncConfig();
            sourceSyncConfig.setName(sourceConfig.getName());
            sourceSyncConfig.setDestinations(sync);
            sourceSyncConfig.setSourceConfig(sourceConfig);
            sourceSyncConfig.setParameters(sourceConfig.getParameters());

            addSourceSyncConfig(sourceSyncConfig);
        }
    }

    public SourceConfig removeSourceConfig(String name) {
        return (SourceConfig)sourceConfigs.remove(name);
    }

    public SourceConfig getSourceConfig(String name) {
        return (SourceConfig)sourceConfigs.get(name);
    }

    public SourceConfig getSourceConfig(SourceMapping sourceMapping) {
        return getSourceConfig(sourceMapping.getSourceName());
    }

    public Collection getSourceConfigs() {
        return sourceConfigs.values();
    }

    public void renameSourceConfig(SourceConfig sourceConfig, String newName) {
        if (sourceConfig == null) return;
        if (sourceConfig.getName().equals(newName)) return;

        sourceConfigs.remove(sourceConfig.getName());
        sourceConfigs.put(newName, sourceConfig);
    }

    public void modifySourceConfig(String name, SourceConfig newSourceConfig) {
        SourceConfig sourceConfig = (SourceConfig)sourceConfigs.get(name);
        sourceConfig.copy(newSourceConfig);
    }

    public void addSourceSyncConfig(SourceSyncConfig sourceSyncConfig) {
        sourceSyncConfigs.put(sourceSyncConfig.getName(), sourceSyncConfig);
    }

    public SourceSyncConfig removeSourceSyncConfig(String name) {
        return (SourceSyncConfig) sourceSyncConfigs.remove(name);
    }

    public SourceSyncConfig getSourceSyncConfig(String name) {
        return (SourceSyncConfig) sourceSyncConfigs.get(name);
    }

    public Collection<SourceSyncConfig> getSourceSyncConfigs() {
        return sourceSyncConfigs.values();
    }

}
