package org.safehaus.penrose.source;

import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;

/**
 * @author Administrator
 */
public class SourceSyncConfig implements Cloneable {

    private boolean enabled = true;

    private String name;
    private String destinations;
    private SourceConfig sourceConfig;

    /**
	 * Parameters.
	 */
	public Map<String,String> parameters = new TreeMap<String,String>();

	public Collection getParameterNames() {
		return parameters.keySet();
	}

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void setParameters(Map<String,String> parameters) {
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        SourceSyncConfig sourceSyncConfig = (SourceSyncConfig)object;
        if (!equals(name, sourceSyncConfig.name)) return false;
        if (!equals(destinations, sourceSyncConfig.destinations)) return false;
        if (!equals(parameters, sourceSyncConfig.parameters)) return false;

        return true;
    }

    public void copy(SourceSyncConfig sourceSyncConfig) {
        name = sourceSyncConfig.name;
        destinations = sourceSyncConfig.destinations;

        parameters = new TreeMap<String,String>();
        parameters.putAll(sourceSyncConfig.parameters);
    }

    public Object clone() throws CloneNotSupportedException {
        SourceSyncConfig sourceSyncConfig = (SourceSyncConfig)super.clone();
        sourceSyncConfig.copy(this);
        return sourceSyncConfig;
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public String getDestinations() {
        return destinations;
    }

    public void setDestinations(String destinations) {
        this.destinations = destinations;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
