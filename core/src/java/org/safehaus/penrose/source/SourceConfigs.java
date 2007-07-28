package org.safehaus.penrose.source;

import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SourceConfigs implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private Map<String,SourceConfig> sourceConfigs = new LinkedHashMap<String,SourceConfig>();
    private Map<String,SourceSyncConfig> sourceSyncConfigs = new LinkedHashMap<String,SourceSyncConfig>();

    public void addSourceConfig(SourceConfig sourceConfig) {

        String sourceName = sourceConfig.getName();

        if (debug) {
            log.debug("Adding source "+sourceName+":");
            for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {
                log.debug(" - "+fieldConfig.getName()+": "+fieldConfig.getType());
            }
        }

        sourceConfigs.put(sourceName, sourceConfig);

        String sync = sourceConfig.getParameter("sync");
        if (sync != null) {

            log.debug("Sync source with "+sync+".");

            SourceSyncConfig sourceSyncConfig = new SourceSyncConfig();
            sourceSyncConfig.setName(sourceName);
            sourceSyncConfig.setDestinations(sync);
            sourceSyncConfig.setSourceConfig(sourceConfig);
            sourceSyncConfig.setParameters(sourceConfig.getParameters());

            addSourceSyncConfig(sourceSyncConfig);
        }
    }

    public SourceConfig removeSourceConfig(String sourceName) {
        return sourceConfigs.remove(sourceName);
    }

    public SourceConfig getSourceConfig(String name) {
        return sourceConfigs.get(name);
    }

    public SourceConfig getSourceConfig(SourceMapping sourceMapping) {
        return getSourceConfig(sourceMapping.getSourceName());
    }

    public Collection<SourceConfig> getSourceConfigs() {
        return sourceConfigs.values();
    }

    public void renameSourceConfig(SourceConfig sourceConfig, String newName) {
        if (sourceConfig == null) return;
        if (sourceConfig.getName().equals(newName)) return;

        sourceConfigs.remove(sourceConfig.getName());
        sourceConfigs.put(newName, sourceConfig);
    }

    public void modifySourceConfig(String name, SourceConfig newSourceConfig) throws Exception {
        SourceConfig sourceConfig = sourceConfigs.get(name);
        sourceConfig.copy(newSourceConfig);
    }

    public void addSourceSyncConfig(SourceSyncConfig sourceSyncConfig) {
        sourceSyncConfigs.put(sourceSyncConfig.getName(), sourceSyncConfig);
    }

    public SourceSyncConfig removeSourceSyncConfig(String name) {
        return sourceSyncConfigs.remove(name);
    }

    public SourceSyncConfig getSourceSyncConfig(String name) {
        return sourceSyncConfigs.get(name);
    }

    public Collection<SourceSyncConfig> getSourceSyncConfigs() {
        return sourceSyncConfigs.values();
    }

    public Collection<FieldMapping> getSearchableFields(SourceMapping sourceMapping) {
        SourceConfig sourceConfig = getSourceConfig(sourceMapping.getSourceName());

        Collection<FieldMapping> results = new ArrayList<FieldMapping>();
        for (FieldMapping fieldMapping : sourceMapping.getFieldMappings()) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());
            if (fieldConfig == null) continue;
            if (!fieldConfig.isSearchable()) continue;
            results.add(fieldMapping);
        }

        return results;
    }

    public Object clone() throws CloneNotSupportedException {
        SourceConfigs sources = (SourceConfigs)super.clone();

        sources.sourceConfigs = new LinkedHashMap<String,SourceConfig>();
        for (SourceConfig sourceConfig : sourceConfigs.values()) {
            sources.sourceConfigs.put(sourceConfig.getName(), (SourceConfig)sourceConfig.clone());
        }

        sources.sourceSyncConfigs = new LinkedHashMap<String,SourceSyncConfig>();
        for (SourceSyncConfig sourceSyncConfig : sourceSyncConfigs.values()) {
            sources.sourceSyncConfigs.put(sourceSyncConfig.getName(), (SourceSyncConfig)sourceSyncConfig.clone());
        }

        return sources;
    }
}
