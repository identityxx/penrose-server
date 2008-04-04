package org.safehaus.penrose.source;

import org.safehaus.penrose.directory.SourceMapping;
import org.safehaus.penrose.directory.FieldMapping;
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

    public final static String SYNC = "sync";

    protected Map<String,SourceConfig> sourceConfigs                             = new LinkedHashMap<String,SourceConfig>();
    protected Map<String,Collection<SourceConfig>> sourceConfigsByConnectionName = new LinkedHashMap<String,Collection<SourceConfig>>();
    protected Map<String,SourceSyncConfig> sourceSyncConfigs                     = new LinkedHashMap<String,SourceSyncConfig>();

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

        Collection<String> destinations = getSourceSyncNames(sourceConfig);
        if (!destinations.isEmpty()) {

            log.debug("Sync source with "+destinations+".");

            SourceSyncConfig sourceSyncConfig = new SourceSyncConfig();
            sourceSyncConfig.setName(sourceName);
            sourceSyncConfig.setDestinations(destinations);
            sourceSyncConfig.setSourceConfig(sourceConfig);
            sourceSyncConfig.setParameters(sourceConfig.getParameters());

            addSourceSyncConfig(sourceSyncConfig);
        }
    }

    public Collection<String> getSourceNames() {
        return sourceConfigs.keySet();
    }
    
    public Collection<String> getSourceSyncNames(SourceConfig sourceConfig) {

        Collection<String> list = new ArrayList<String>();

        String sync = sourceConfig.getParameter(SourceConfigManager.SYNC);
        if (sync == null) return list;

        StringTokenizer st = new StringTokenizer(sync, ", ");
        while (st.hasMoreTokens()) {
            String name = st.nextToken();
            list.add(name);
        }

        return list;
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

    public SourceConfig getSourceConfig(SourceMapping sourceMapping) {
        return getSourceConfig(sourceMapping.getSourceName());
    }

    public Collection<SourceConfig> getSourceConfigs() {
        return sourceConfigs.values();
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
        SourceConfigManager sources = (SourceConfigManager)super.clone();

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
