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

    public final static long serialVersionUID = 1L;

    protected Map<String,SourceConfig> sourceConfigs                             = new LinkedHashMap<String,SourceConfig>();
    protected Map<String,Collection<SourceConfig>> sourceConfigsByConnectionName = new LinkedHashMap<String,Collection<SourceConfig>>();

    public void addSourceConfig(SourceConfig sourceConfig) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        String sourceName = sourceConfig.getName();

        if (debug) log.debug("Adding source \""+sourceName+"\".");

        validate(sourceConfig);
        
        sourceConfigs.put(sourceName, sourceConfig);

        String connectionName = sourceConfig.getConnectionName();
        if (connectionName != null) {
            Collection<SourceConfig> list = sourceConfigsByConnectionName.get(connectionName);
            if (list == null) {
                list = new ArrayList<SourceConfig>();
                sourceConfigsByConnectionName.put(connectionName, list);
            }
            list.add(sourceConfig);
        }
    }

    public void validate(SourceConfig sourceConfig) throws Exception {

        String sourceName = sourceConfig.getName();

        if (sourceName == null || "".equals(sourceName)) {
            throw new Exception("Missing source name.");
        }

        char startingChar = sourceName.charAt(0);
        if (!Character.isLetter(startingChar)) {
            throw new Exception("Invalid source name: "+sourceName);
        }

        for (int i = 1; i<sourceName.length(); i++) {
            char c = sourceName.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') continue;
            throw new Exception("Invalid source name: "+sourceName);
        }

        if (sourceConfigs.containsKey(sourceName)) {
            throw new Exception("Source "+sourceName+" already exists.");
        }
    }

    public Collection<String> getSourceNames() {
        return sourceConfigs.keySet();
    }

    public void renameSourceConfig(String name, String newName) throws Exception {
        SourceConfig sourceConfig = sourceConfigs.remove(name);
        sourceConfig.setName(newName);
        sourceConfigs.put(newName, sourceConfig);
    }

    public void updateSourceConfig(SourceConfig sourceConfig) throws Exception {

        String sourceName = sourceConfig.getName();

        SourceConfig oldSourceConfig = sourceConfigs.get(sourceName);
        if (oldSourceConfig == null) {
            throw new Exception("Source "+sourceName+" not found.");
        }

        String oldConnectionName = oldSourceConfig.getConnectionName();
        String connectionName = sourceConfig.getConnectionName();

        oldSourceConfig.copy(sourceConfig);

        if (oldConnectionName != null && !oldConnectionName.equals(connectionName)
                || connectionName != null && !connectionName.equals(oldConnectionName)) {

            if (oldConnectionName != null) {
                Collection<SourceConfig> list = sourceConfigsByConnectionName.get(oldConnectionName);
                if (list != null) {
                    list.remove(oldSourceConfig);
                    if (list.isEmpty()) sourceConfigsByConnectionName.remove(oldConnectionName);
                }
            }

            if (connectionName != null) {
                Collection<SourceConfig> list = sourceConfigsByConnectionName.get(connectionName);
                if (list == null) {
                    list = new ArrayList<SourceConfig>();
                    sourceConfigsByConnectionName.put(connectionName, list);
                }
                list.add(sourceConfig);
            }
        }
    }

    public SourceConfig removeSourceConfig(String sourceName) {
        SourceConfig sourceConfig = sourceConfigs.remove(sourceName);

        String connectionName = sourceConfig.getConnectionName();
        if (connectionName != null) {
            Collection<SourceConfig> list = sourceConfigsByConnectionName.get(connectionName);
            if (list != null) {
                list.remove(sourceConfig);
                if (list.isEmpty()) sourceConfigsByConnectionName.remove(connectionName);
            }
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
