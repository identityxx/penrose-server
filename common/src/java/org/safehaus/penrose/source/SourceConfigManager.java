package org.safehaus.penrose.source;

import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.directory.EntryFieldConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class SourceConfigManager implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(SourceConfigManager.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    protected Map<String,SourceConfig> sourceConfigs                             = new LinkedHashMap<String,SourceConfig>();
    protected Map<String,Collection<SourceConfig>> sourceConfigsByConnectionName = new LinkedHashMap<String,Collection<SourceConfig>>();

    public void addSourceConfig(SourceConfig sourceConfig) {

        String sourceName = sourceConfig.getName();
/*
        if (debug) {
            log.debug("Adding source "+sourceName+":");
            for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {
                log.debug(" - "+fieldConfig.getName()+": "+fieldConfig.getType());
            }
        }
*/
        sourceConfigs.put(sourceName, sourceConfig);

        String connectionName = sourceConfig.getConnectionName();
        Collection<SourceConfig> list = sourceConfigsByConnectionName.get(connectionName);
        if (list == null) {
            list = new ArrayList<SourceConfig>();
            sourceConfigsByConnectionName.put(connectionName, list);
        }
        list.add(sourceConfig);
    }

    public Collection<String> getSourceNames() {
        return sourceConfigs.keySet();
    }

    public void updateSourceConfig(String sourceName, SourceConfig sourceConfig) throws Exception {

        SourceConfig oldSourceConfig = sourceConfigs.get(sourceName);

        String oldConnectionName = oldSourceConfig.getConnectionName();
        String newConnectionName = sourceConfig.getConnectionName();

        oldSourceConfig.copy(sourceConfig);

        if (!sourceName.equals(sourceConfig.getName())) {
            sourceConfigs.remove(sourceName);
            sourceConfigs.put(sourceConfig.getName(), sourceConfig);
        }

        if (!oldConnectionName.equals(newConnectionName)) {
            Collection<SourceConfig> list = sourceConfigsByConnectionName.get(oldConnectionName);
            if (list != null) {
                list.remove(oldSourceConfig);
                if (list.isEmpty()) sourceConfigsByConnectionName.remove(oldConnectionName);
            }

            list = sourceConfigsByConnectionName.get(newConnectionName);
            if (list == null) {
                list = new ArrayList<SourceConfig>();
                sourceConfigsByConnectionName.put(newConnectionName, list);
            }
            list.add(sourceConfig);
        }
    }

    public SourceConfig removeSourceConfig(String sourceName) {
        SourceConfig sourceConfig = sourceConfigs.remove(sourceName);

        String connectionName = sourceConfig.getConnectionName();
        Collection<SourceConfig> list = sourceConfigsByConnectionName.get(connectionName);
        if (list != null) {
            list.remove(sourceConfig);
            if (list.isEmpty()) sourceConfigsByConnectionName.remove(connectionName);
        }

        return sourceConfig;
    }

    public SourceConfig getSourceConfig(String name) {
        return sourceConfigs.get(name);
    }

    public Collection<SourceConfig> getSourceConfigsByConnectionName(String connectionName) {
        return sourceConfigsByConnectionName.get(connectionName);
    }

    public SourceConfig getSourceConfig(EntrySourceConfig entrySourceConfig) {
        return getSourceConfig(entrySourceConfig.getSourceName());
    }

    public Collection<SourceConfig> getSourceConfigs() {
        return sourceConfigs.values();
    }

    public Collection<EntryFieldConfig> getSearchableFields(EntrySourceConfig entrySourceConfig) {
        SourceConfig sourceConfig = getSourceConfig(entrySourceConfig.getSourceName());

        Collection<EntryFieldConfig> results = new ArrayList<EntryFieldConfig>();
        for (EntryFieldConfig entryFieldConfig : entrySourceConfig.getFieldConfigs()) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(entryFieldConfig.getName());
            if (fieldConfig == null) continue;
            if (!fieldConfig.isSearchable()) continue;
            results.add(entryFieldConfig);
        }

        return results;
    }

    public Object clone() throws CloneNotSupportedException {
        SourceConfigManager sources = (SourceConfigManager)super.clone();

        sources.sourceConfigs = new LinkedHashMap<String,SourceConfig>();
        for (SourceConfig sourceConfig : sourceConfigs.values()) {
            sources.sourceConfigs.put(sourceConfig.getName(), (SourceConfig)sourceConfig.clone());
        }

        return sources;
    }
}
